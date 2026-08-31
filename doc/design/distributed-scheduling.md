# Distributed scheduling — what to explore

**Status: open. Exploration notes, not a design.**

Can the scheduler run workers across machines rather than only threads within one
process? This is a list of the questions that have to be answered first, written
against the architecture as of 0.3.0. It deliberately proposes nothing — the
point is to record where the real difficulty sits so the exploration does not
start by rediscovering it.

A previous version of this document was lost and was badly out of date anyway.
Prefer re-deriving from the code over trusting anything here that no longer
matches it.

## The crux: dispatch is synchronous

`FyflowScheduler`'s dispatch loop acquires resources synchronously. That single
property is why two pools cannot both pass a capacity check in the same tick, and
why no async or "strict" group type was ever needed — a `StrictLimitGroup` was
in fact built and then removed as unnecessary.

Distribution breaks that assumption at its root. The moment admission requires a
round trip, `canRun()` / `onStart()` stop being a decision and become a request.
Everything below is downstream of how that is resolved, so resolve it first.

The available shapes, roughly in increasing order of difficulty:

1. **Central scheduler, remote workers.** One process keeps the dispatch loop and
   all group state; only worker execution is remote. Preserves synchronous
   admission entirely. Limited by the single scheduler's throughput and its
   availability.
2. **Sharded schedulers, disjoint groups.** Several schedulers, each owning
   resources nothing else contends for. Synchronous admission survives *within*
   a shard. Requires that the group graph actually partitions.
3. **Multiple schedulers sharing groups.** Needs distributed admission — leases,
   a coordinator, or accepting overshoot. This is where the current design's
   guarantees stop applying.

Shape 1 is the one worth costing first. It may well be sufficient, and it is the
only one that changes no scheduling semantics.

## What is already close

Worth knowing before assuming this is a rewrite:

- **The worker protocol is already a wire protocol in embryo.** Scheduler and
  worker communicate by explicit messages — `init` / `run` / `teardown` inbound,
  and `init` / `result` / `error` / `progress` / `spawn_task` /
  `setup_started` / `setup_completed` outbound. Nothing about it assumes shared
  memory. `ThreadWrapper` and `InlineWrapper` are two transports for it already;
  a remote transport would be a third.
- **Payloads already cross a serialization boundary** for threaded workers, so
  the constraint is established rather than new.
- **Settlement is already idempotent per attempt** (0.2.x). That work is a
  precondition for at-least-once delivery across a network, where a result can
  arrive twice or a node can die mid-task.
- **Resource state is already an event stream** (`resource.acquired` /
  `released` / `blocked` / `unblocked`, 0.3.0). If group state ever has to be
  replicated, this is the log to replicate.

## What is squarely in the way

- **Group state is in-process.** `ConcurrentLimitGroup` is a counter in a field;
  `RateLimitGroup` holds timestamps; `KeyedRateLimitGroup` holds a map. All are
  authoritative only within one process.
- **Rate limits are the hard case, not concurrency limits.** Concurrency groups
  are already documented as optimistic and may briefly exceed their limit by up
  to `maxThreads × maxConcurrentTasks`, so a distributed version overshooting
  further is a change of degree. A rate limit that exists to respect somebody
  else's API quota cannot be approximate in the same way — overshoot there is a
  breach, not a tolerance. Decide whether distributed rate limiting is in scope
  at all, or whether such groups pin their tasks to one node.
- **Resident groups are node-local by nature and that is a feature.**
  `residentGroups` gates something a *worker* holds for its lifetime — VRAM for a
  loaded model. That is a property of one physical machine. Keeping resident
  admission strictly node-local is probably right, and probably simplifies the
  problem rather than complicating it. Confirm before generalising it.
- **Head-of-line admission assumes one queue.** A 20-unit waiter blocking four
  2-unit pools is currently a deliberate anti-starvation property. Across nodes
  it becomes a question about whose queue, and whether the guarantee survives.
- **Lineage is scheduler-side.** `onCompleteDescendants()` tracks a
  parent → children map built from `context.spawnTask()`. If a remote worker can
  spawn, the lineage spans nodes and so does the completion wait.
- **Completion detection.** `scheduler.completed` accounts for tasks blocked on a
  group. Distributed, "nothing is running anywhere and nothing can become
  runnable" is a genuinely harder predicate — and the periodic-retry stall fixed
  in 0.3.0 is a reminder of how easily that goes wrong even in one process.
- **Events are local.** `task.*`, `worker.*` and `resource.*` are dispatched on
  in-process `EventTarget`s. Observability across nodes needs aggregation, and
  consumers currently assume they see every event.

## Questions to answer before designing anything

1. What is actually being solved — throughput beyond one machine, resources that
   only exist on specific machines (GPUs), or fault tolerance? These pull toward
   different shapes, and only the second clearly needs more than shape 1.
2. Which delivery guarantee: at-least-once with idempotent settlement, or an
   attempt at exactly-once? The former is likely, given the existing settlement
   work.
3. What happens when a node disappears mid-task? Today a dead worker requeues.
   A dead node means requeue elsewhere plus deciding whether a task that may
   still be running somewhere can be re-dispatched.
4. Does the public API change, or is remoteness entirely a transport concern
   behind `WorkerManager`? If the latter, most of `AGENTS.md` stays true, which
   is a strong argument for that framing.
5. What is the smallest useful thing to build — probably shape 1 with a single
   remote transport and concurrency groups only, deferring rate limits and
   multi-scheduler entirely.

## Before this document is deleted

Per the `doc/design/` convention this is transient. If the exploration concludes,
move the durable conclusions out before deleting: API contracts to `AGENTS.md`,
operational gotchas to `CLAUDE.md`, and anything measured into a test. If it is
abandoned instead, say so here and delete it — an open question left lying around
reads as a commitment.
