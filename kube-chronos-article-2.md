# Building a Production-Grade Kubernetes Scaling Controller in Go — Every Pitfall, Fixed

*Why KEDA wasn't the answer, and what it took to build kube-chronos correctly.*

---

There is a category of Kubernetes problem that feels simple until you sit down to build it properly.

The requirement sounds almost trivial: look for a custom annotation on Deployments and StatefulSets, parse a cron schedule and a replica count out of it, scale the workload up at the start time and back down at the end time. Dozens of teams have built this. Most of them have bugs they do not know about yet.

This is the story of building **kube-chronos** — a controller that does exactly that, and of going back through the code until every subtle failure mode was either fixed or explicitly documented.

---

## Why not KEDA?

The obvious first answer was [KEDA](https://keda.sh/) — the widely-adopted Kubernetes Event Driven Autoscaler. KEDA has a `ScaledObject` resource with a built-in cron trigger. On paper it is a perfect fit.

In practice it was the wrong tool for our situation, and the friction surfaced quickly.

KEDA's cron scaler works by replacing the HPA. A `ScaledObject` assumes ownership of scaling for its target workload. That means removing the existing HPAs — and in our case those HPAs were load-bearing. They were tuned, they had memory and CPU targets, they were referenced by other tooling. Ripping them out to replace them with `ScaledObjects` was a larger change than the problem warranted.

There was a subtler issue too. We use **ArgoCD** for GitOps. ArgoCD continuously compares the live cluster state to the Git repository, and it treats any divergence as a sync drift. A KEDA `ScaledObject` actively mutates the replica count on the target workload — which means ArgoCD immediately sees the live Deployment diverge from the Git-managed spec. You can work around this by adding an `ignoreDifferences` block to the ArgoCD `Application` manifest:

```yaml
ignoreDifferences:
  - group: apps
    kind: Deployment
    jsonPointers:
      - /spec/replicas
```

That is not a terrible config change, but it has a cost: you are now permanently suppressing replica drift detection for every Deployment in that application. If something else accidentally changes the replica count — a botched rollout, a manual `kubectl scale` that was never reverted — ArgoCD will not catch it. You trade one solved problem for a permanently reduced safety net.

We wanted cron-based scaling without touching HPA ownership and without loosening ArgoCD's drift detection. That meant writing our own controller.

---

## What the annotation looks like

A deployment opts into scheduled scaling by adding a single annotation:

```json
"kube-chronos/schedule": {
  "timezone": "America/New_York",
  "startTime": "0 6 * * *",
  "endTime":   "0 20 * * *",
  "desiredReplicas": 10
}
```

At 06:00 Eastern, kube-chronos scales the deployment to 10 replicas. At 20:00 it scales back to the HPA's `minReplicas`. The HPA is never removed or replaced — it continues to autoscale within the replica count kube-chronos sets. If the annotation is missing, the workload is ignored. If `desiredReplicas` falls outside the HPA's `[min, max]` band, the annotation is rejected with a log error and nothing is touched.

The scope is intentionally narrow: only workloads in Istio-monitored namespaces are eligible. Everything else is invisible to the controller.

---

## The architecture in one paragraph

kube-chronos is a standard Kubernetes informer-based reconciler. A `SharedInformerFactory` watches Namespaces, Deployments, StatefulSets, and HPAs. Event handlers push string keys onto a rate-limiting workqueue. A single worker goroutine drains the queue, calling `reconcile()` for each key. `reconcile()` reads current state from the lister caches and installs or removes entries on a single global cron scheduler.

No CRDs. No operator frameworks. One binary.

---

## Lesson 1 — Event handlers must be fast

The first version of our handlers did real work — lister lookups, annotation parsing, some light validation — before pushing to the queue. It worked fine locally. In the staging cluster, with a few hundred deployments, it was fine. We moved toward a larger cluster and hit an OOM kill we did not immediately understand.

The root cause: informer event handlers process events **in serial**. A single goroutine runs every `AddFunc`, `UpdateFunc`, and `DeleteFunc` call, one at a time. When the controller starts and the initial LIST returns thousands of objects, that goroutine works through them sequentially — and while it does, any events that arrive but cannot yet be delivered are held in an **unbounded ring buffer** inside the informer. A handler that blocks, even briefly on a cache lookup, causes that buffer to grow without any ceiling. Under enough load it OOMs the process. Kubernetes restarts the pod, the initial LIST floods the buffer again, and the cycle repeats — a loop we could not break out of until we understood why it was happening.

The fix is strict: every handler does exactly one thing — a fast in-memory namespace lookup and a queue push:

```go
c.depInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
    AddFunc: func(obj any) { c.enqueueDeployment(obj) },
    UpdateFunc: func(oldObj, newObj any) {
        oldDep, okOld := extractDeployment(oldObj)
        newDep, okNew := extractDeployment(newObj)
        if !okOld || !okNew {
            return
        }
        // Same ResourceVersion = resync call, nothing changed on disk.
        if oldDep.ResourceVersion == newDep.ResourceVersion {
            return
        }
        // Only re-enqueue if our annotation actually changed.
        if oldDep.Annotations[AnnotationKey] == newDep.Annotations[AnnotationKey] {
            return
        }
        c.enqueueDeployment(newObj)
    },
    DeleteFunc: func(obj any) { c.enqueueDeployment(obj) },
})
```

And `enqueueDeployment` itself — a namespace lookup and a queue push, nothing more:

```go
func (c *Controller) enqueueDeployment(obj any) {
    dep, ok := extractDeployment(obj)
    if !ok {
        return
    }
    // Pre-filter: skip non-Istio namespaces before they touch the queue.
    if !c.isIstioNamespace(dep.Namespace) {
        return
    }
    c.queue.Add(WorkloadKey{
        Kind:      KindDeployment,
        Namespace: dep.Namespace,
        Name:      dep.Name,
    }.String())
}
```

No API calls. No parsing. No validation. All expensive work happens in `reconcile()`, called from the worker goroutine — not from the event handler. The workqueue also gives deduplication for free: ten events for the same deployment coalesce into a single reconcile call.

---

## Lesson 2 — Level-driven, not edge-driven

The [official Kubernetes controller guidelines](https://github.com/kubernetes/community/blob/main/contributors/devel/sig-api-machinery/controllers.md) are unambiguous on this:

> Level driven, not edge driven. You can't count on having seen a transition, only that you now observe the current state.

The most common class of controller bug is one that treats events as reliable state transitions. Code that asks "did the annotation change?" instead of "what does the annotation say right now?" will work perfectly in a quiet dev cluster and fail silently in production — under network partitions, during API server restarts, and across controller rollouts. Informers are not guaranteed to deliver every intermediate state. They are only guaranteed to eventually deliver the current state.

The correct mental model: whatever you observe right now is the world. Act on that, and only that.

The `reconcile()` function reads namespace state, annotation value, and HPA bounds fresh from the cache on every single invocation:

```go
func (c *Controller) reconcile(ctx context.Context, rawKey string) error {
    key, ok := ParseWorkloadKey(rawKey)
    if !ok {
        utilruntime.HandleError(fmt.Errorf("malformed workload key %q", rawKey))
        return nil
    }

    // 1. Always read current namespace state — never trust the triggering event.
    ns, err := c.nsLister.Get(key.Namespace)
    if err != nil {
        if apierrors.IsNotFound(err) {
            c.removeSchedule(rawKey)
            return nil
        }
        return err // transient — retry with exponential backoff
    }
    if !isIstioMonitoredNamespace(ns) {
        c.removeSchedule(rawKey)
        return nil
    }

    // 2. Always read current annotation state.
    annotationRaw, exists, err := c.getAnnotation(key)
    if !exists {
        c.removeSchedule(rawKey)
        return nil
    }

    // 3. Always validate against current HPA bounds.
    parsed, minReplicas, err := c.validateSchedule(key, annotationRaw)
    if err != nil {
        // User config error — log via HandleError, do not retry.
        utilruntime.HandleError(fmt.Errorf("invalid annotation on %s: %w", rawKey, err))
        c.removeSchedule(rawKey)
        return nil
    }

    // 4. Performance gate only — not a correctness gate.
    //    Removing this check would not break the controller; it would just
    //    reinstall identical schedules on every resync.
    if existing := c.schedules[rawKey]; existing != nil &&
        existing.annotationRaw == annotationRaw &&
        existing.minReplicas == minReplicas {
        return nil
    }

    // 5. (Re-)install based purely on current observed state.
    c.removeSchedule(rawKey)
    sched, err := c.installSchedule(key, annotationRaw, parsed, minReplicas)
    ...
}
```

This design makes the 30-minute informer resync a free safety net: even if a watch event was dropped, the controller will self-correct within half an hour.

---

## Lesson 3 — SetTransform dramatically reduces memory

The informer stores a complete copy of every watched object in its in-memory cache. For a Deployment in a real cluster, this includes the full pod template — containers, environment variables, volume mounts, init containers, readiness probes. That can be 50–100 KB per object. At 10,000 Deployments, the cache alone consumes up to 1 GB.

`SetTransform` is a hook that runs on each object before it enters the cache. kube-chronos uses it to strip everything the controller will never read. Rather than mutating the original object in place, we construct a fresh minimal struct — this matters because in-place mutation can leave dangling sub-object pointers that prevent GC:

```go
// slimDeploymentObj keeps only what reconcile() actually reads:
// identity fields and our one annotation key.
func slimDeploymentObj(obj any) (any, error) {
    dep, ok := obj.(*appsv1.Deployment)
    if !ok {
        return obj, nil
    }
    slim := &appsv1.Deployment{
        ObjectMeta: metav1.ObjectMeta{
            Namespace:       dep.Namespace,
            Name:            dep.Name,
            ResourceVersion: dep.ResourceVersion,
            Generation:      dep.Generation,
        },
    }
    // Preserve only our annotation key; discard everything else.
    if v, ok := dep.Annotations[AnnotationKey]; ok {
        slim.Annotations = map[string]string{AnnotationKey: v}
    }
    return slim, nil
}
```

The transforms are registered before the informer factory starts:

```go
c.depInformer.Informer().SetTransform(slimDeploymentObj)
c.stsInformer.Informer().SetTransform(slimStatefulSetObj)
c.hpaInformer.Informer().SetTransform(slimHPAObj)
c.nsInformer.Informer().SetTransform(slimNamespaceObj)
```

After transformation, each cached Deployment is roughly 200 bytes. 10,000 Deployments now occupy about 2 MB instead of up to 1 GB.

Two rules for a correct transform function: it must be **idempotent** (the informer re-runs it on every resync), and it must not mutate fields used by indexers. A real Kubernetes bug — [kubernetes#124352](https://github.com/kubernetes/kubernetes/pull/124352) — was caused by a non-idempotent transform function producing a data race in `kube-controller-manager` and the scheduler during resync. Constructing a fresh struct instead of mutating in place satisfies both rules automatically.

---

## Lesson 4 — The missed-fire problem

This is the failure mode most scheduling implementations quietly ignore.

```
06:00  scale-up cron fires   → controller is DOWN
06:05  controller restarts   → schedule installed, nothing scaled
20:00  scale-down fires      → no-op (already at minReplicas)
```

The deployment runs at minimum capacity all day. No error is logged. No alert fires.

The fix requires knowing whether the current wall-clock time falls inside the schedule's active window. `robfig/cron` — the library we use for the scheduler — does not expose a `Prev()` method for walking back in time, so we bring in `github.com/gitploy-io/cronexpr` specifically for this computation:

```go
func isInsideActiveWindow(now time.Time, startExpr, endExpr string) (bool, error) {
    startSchedule, err := cronexpr.NewExpression(startExpr)
    if err != nil {
        return false, fmt.Errorf("cronexpr: invalid startTime %q: %w", startExpr, err)
    }
    endSchedule, err := cronexpr.NewExpression(endExpr)
    if err != nil {
        return false, fmt.Errorf("cronexpr: invalid endTime %q: %w", endExpr, err)
    }

    lastStart := startSchedule.Prev(now)
    lastEnd   := endSchedule.Prev(now)

    switch {
    case lastStart.IsZero() && lastEnd.IsZero():
        return false, nil // neither has ever fired — brand-new schedule
    case lastStart.IsZero():
        return false, nil // end fired but start never has
    case lastEnd.IsZero():
        return true, nil  // start fired but end never has — inside window
    default:
        return lastStart.After(lastEnd), nil
    }
}
```

On every schedule install, `enforceCurrentState` calls this check and scales immediately if needed:

```go
func (c *Controller) enforceCurrentState(
    rawKey string, key WorkloadKey,
    parsed ScheduleAnnotation, minReplicas int32,
) error {
    loc, _ := time.LoadLocation(parsed.Timezone)
    now := time.Now().In(loc)

    active, err := isInsideActiveWindow(now, parsed.StartTime, parsed.EndTime)
    if err != nil {
        return err
    }

    ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
    defer cancel()

    if active {
        c.log.Info().Str("workload", rawKey).Int32("scalingTo", parsed.DesiredReplicas).
            Msg("inside active window — enforcing desiredReplicas immediately")
        return c.scaleWorkload(ctx, key, parsed.DesiredReplicas)
    }

    c.log.Info().Str("workload", rawKey).Int32("scalingTo", minReplicas).
        Msg("outside active window — enforcing minReplicas")
    return c.scaleWorkload(ctx, key, minReplicas)
}
```

This runs on every `installSchedule` — not just on startup. If someone edits the annotation during off-hours, the controller immediately enforces `minReplicas`. If they edit it inside an active window, `desiredReplicas` is applied without waiting for the next cron fire. The annotation is always the source of truth.

---

## Lesson 5 — Three shutdown bugs in one function

The shutdown sequence had three bugs, each invisible in normal operation.

**Bug 1 — The zombie process.**

`Run()` is launched in a goroutine. If `WaitForCacheSync` fails and `Run()` returns early, `main()` blocks forever on `<-ctx.Done()`. The process shows as Running. Nothing is happening.

```go
// BROKEN: Run() exits early, main() never unblocks
go ctrl.Run(ctx)
<-ctx.Done()      // ← stuck here forever
```

The fix: pass `cancel` into `Run()` and call it as the first deferred statement. Any exit — sync failure, panic, or normal shutdown — cancels the context, unblocks `main()`, and flows through `Shutdown()`.

```go
func (c *Controller) Run(ctx context.Context, cancel context.CancelFunc) {
    defer cancel()               // always unblock main(), on every exit path
    defer close(c.workerStopped)
    defer utilruntime.HandleCrash()

    c.factory.Start(ctx.Done())
    c.cron.Start()

    // Queue-shutdown goroutine launched before sync check so it fires
    // on every exit path, not just the success path.
    go func() {
        <-ctx.Done()
        c.queue.ShutDown()
    }()

    if !cache.WaitForCacheSync(ctx.Done(), ...) {
        c.log.Error().Msg("cache sync failed")
        return // defer cancel() fires → main() unblocks → Shutdown() runs
    }
    ...
}
```

**Bug 2 — The data race.**

`Shutdown()` called `clear(schedules)` unconditionally. If the shutdown timeout fired before the reconcile worker finished its current call, `clear` would race against the worker on the same map.

```go
// BROKEN: clear races the worker if timeout fires first
select {
case <-c.workerStopped:
case <-ctx.Done():
}
clear(c.schedules) // ← DATA RACE if worker is still running
```

```go
// FIXED: only clear when worker has cleanly exited
workerClean := false
select {
case <-c.workerStopped:
    workerClean = true
case <-ctx.Done():
    c.log.Warn().Msg("timed out waiting for reconcile worker")
}

stopCtx := c.cron.Stop()
select {
case <-stopCtx.Done():
case <-ctx.Done():
}

if workerClean {
    clear(c.schedules)
}
```

**Bug 3 — The timer leak.**

The panic-restart back-off used `time.After`, which creates a `*time.Timer` that cannot be stopped. If `ctx.Done()` wins the select, the timer lives on the heap for another second.

```go
// LEAKY: timer cannot be GC'd until it fires
case <-time.After(time.Second):
```

```go
// FIXED: explicit Stop() releases the timer immediately
t := time.NewTimer(time.Second)
select {
case <-ctx.Done():
    t.Stop()
    return
case <-t.C:
}
```

---

## Lesson 6 — GC tuning for container environments

`SetTransform` is the dominant GC win — already covered above. Two smaller changes complete the picture.

**`GOMEMLIMIT`** should be set in every containerised Go process, bound to the container's memory limit via the Kubernetes downward API:

```yaml
env:
  - name: GOMEMLIMIT
    valueFrom:
      resourceFieldRef:
        resource: limits.memory
```

Without it, the Go runtime targets a heap of approximately 2× the previous live heap and has no knowledge of the container boundary. If the live heap reaches ~130 Mi in a 256 Mi container, the GC's next target is ~260 Mi — the OOM killer fires before the GC does. With `GOMEMLIMIT` set, the runtime runs more frequent, smaller GCs as the heap approaches the limit rather than letting it overshoot.

**String building.** Two hot-path functions — the workload key built on every enqueue, and the HPA index key built on every schedule validation — used naive concatenation:

```go
// BEFORE: two intermediate heap allocations per call
func (k WorkloadKey) String() string {
    return string(k.Kind) + "/" + k.Namespace + "/" + k.Name
}
```

```go
// AFTER: Grow pre-allocates the exact final size — one allocation
func (k WorkloadKey) String() string {
    var b strings.Builder
    b.Grow(len(k.Kind) + 1 + len(k.Namespace) + 1 + len(k.Name))
    b.WriteString(string(k.Kind))
    b.WriteByte('/')
    b.WriteString(k.Namespace)
    b.WriteByte('/')
    b.WriteString(k.Name)
    return b.String()
}
```

Small saving per call, but `String()` runs tens of thousands of times during a cluster-wide resync.

---

## Health checks without an HTTP server

A controller that never serves traffic does not need `net/http`. Kubernetes `exec` probes run a command inside the container and interpret the exit code. kube-chronos uses its own binary as the probe executable, dispatching on a `probe` subcommand before any controller initialisation happens:

```go
func main() {
    if len(os.Args) == 3 && os.Args[1] == "probe" {
        runProbeCheck(os.Args[2]) // exits 0 or 1 — never returns
    }
    // normal controller startup...
}
```

Two sentinel files in `/tmp` carry the health state. The readiness file is created after cache sync succeeds and removed by a deferred cleanup when the controller exits — ensuring the pod transitions to unready before termination, giving kube-proxy time to drain. The liveness file is touched every 30 seconds by a heartbeat goroutine; the probe checks its modification time against a 90-second threshold.

```yaml
readinessProbe:
  exec:
    command: ["/app/kube-chronos", "probe", "ready"]
  initialDelaySeconds: 15
  periodSeconds: 10

livenessProbe:
  exec:
    command: ["/app/kube-chronos", "probe", "live"]
  initialDelaySeconds: 60
  periodSeconds: 30
  failureThreshold: 3
```

No HTTP server. No extra binary. Works in distroless containers. The heartbeat goroutine exits cleanly when the context is cancelled — no leak.

---

## Goroutine inventory

Part of building this correctly was auditing every goroutine and confirming each one has a bounded exit condition:

| Goroutine | Exit trigger |
|---|---|
| Reconcile worker | Queue drains after shutdown |
| Informer goroutines (×4) | Context cancellation via stop channel |
| Cron scheduler | Explicit `Stop()` in shutdown |
| Queue-shutdown | Context cancellation, exits immediately |
| Heartbeat | Context cancellation in select loop |
| Cron job (transient) | Returns after scale call or 15 s timeout |

None can block indefinitely. The cron jobs carry a 15-second context timeout. `Shutdown()` waits for `cron.Stop()`, which blocks at most 15 seconds. The process always terminates.

---

## What we learned

The interesting insight from building kube-chronos is that the domain logic is genuinely simple. Parsing a cron expression and calling `UpdateScale` takes maybe 50 lines. The remaining 650 are operational correctness: what happens on restart, what happens on timeout, what happens when the cache is stale, what happens when two goroutines reach the same map.

The Kubernetes controller guidelines put it simply: **treat current state as the only truth, and structure code to tolerate any amount of missed history.** An informer that missed a watch event will deliver the correct current state on the next resync. A controller that restarted mid-window can recover by reading the clock and the cache. A shutdown that timed out cleans up what it can and exits.

The bugs that survive code review are almost always the ones that assume continuity: that the controller was running when the annotation was added, that the goroutine will exit before the map is cleared, that the cache was warm when the enqueue happened. They work perfectly in tests. They fail silently at 06:00 when the first cron job fires into a restarting pod.

The code is on GitHub. Run it with `-race`. Read the comments.

---

*kube-chronos is built in Go 1.26 using client-go, robfig/cron, gitploy-io/cronexpr, and zerolog.*
