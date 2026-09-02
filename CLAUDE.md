# FyFlow Scheduler — working on this repository

Runtime-agnostic parallel task scheduler with resource management. Runs on Deno,
Node (>= 22) and in the browser from a single source tree, with platform
builds produced by esbuild.

Tasks are independent: there is no dependency graph. Every task runs as soon as a
worker slot and its resource groups allow.

## Which document to read

Three files, deliberately non-overlapping. Duplicating the API across them is
what made earlier versions of this file rot.

| File | Audience | Contents |
|---|---|---|
| `README.md` | users | introduction, install, the main recipes |
| `AGENTS.md` | agents/consumers | **complete** API reference: exact signatures, defaults, every event, gotchas. Every snippet in it is executed by the docs test suite, and a test asserts it covers every export |
| `CLAUDE.md` (this file) | anyone changing this repo | how the repo is built, tested, released, and what will bite you |
| `TODO_CHECKLIST.md` | anyone changing this repo | active tasks, known issues, the task lifecycle and benchmarking process |
| `doc/design/*.md` | whoever is building that change | plans and analyses. **Transient** — deleted once shipped; see below |

**Do not document the API here.** If a signature or event needs writing down, it
belongs in `AGENTS.md`, where a test enforces it.

## Repository role

This repository is the source of truth for the library and the surface it
publishes from. It was previously a public mirror of a private repo; that repo no
longer exists, and its history is gone. Nothing here depends on it any more —
`scripts/port-from-private.ts` is obsolete and gitignored.

Two packages are published from one source tree, from one `v*` tag:

| Registry | Name | Runtimes |
|---|---|---|
| JSR | `@fyflow/scheduler` | **Deno only** |
| npm | `fyflow-scheduler` | Node and browser |

JSR is deliberately Deno-only. JSR has no build step and rejects conditional
exports — one entrypoint maps to exactly one file for every runtime — and it
serves `.ts` to Deno and transpiled `.js` to everyone else. Node and browser
consumers need the esbuild bundles, which only npm can carry.

## Downstream consumers

This library is no longer only consumed by end users. Sibling libraries in the
same ecosystem depend on the published packages — one building on the resource
model, another importing the scheduler to build flow planning on top of it. That
is invisible from inside this repository, and it changes how two things should
be weighed:

- **Breaking changes have consequences beyond this repo.** Judge them against
  code you cannot see and cannot fix in the same commit. The `ResourceGroup.type`
  widening in 0.3.0 is the reference case: source-breaking for an exhaustive
  switch, deliberately released as a minor, and the reasoning belongs in the
  release commit whenever a call like that is made again.
- **The no-dependencies invariant matters more than it looks.** Consumers
  inherit anything this package depends on, transitively. `jsr:smoke` enforces
  it; do not relax it for convenience.

Feature requests can also arrive as a specification from a consumer rather than
as an issue — the 0.3.0 resource events were designed as a counter-proposal to
one. When that happens, the plan for it belongs in `doc/design/` and the
resulting contract in `AGENTS.md`.

## Architecture

**`core/FyflowScheduler.ts`** — the orchestrator. Task states are
`pending → running → done/failed`; a task that fails with no retries left and is
not optional ends in `user_action`. Dispatch is a synchronous loop, which is what
lets resource admission be race-free without an async group type.

**`core/workerManager.ts`** — pools of workers, task queueing, capacity planning,
idle termination. Capacity is `maxThreads × maxConcurrentTasks`.

**`core/threadWrapper.ts` / `core/inlineWrapper.ts`** — one OS thread per worker,
versus in-process execution for async work. Inline workers consume no thread
capacity.

**`core/resourceEvents.ts`** — the `resource.*` event payload types.

**`groups/`** — `ConcurrentLimitGroup` (optimistic concurrency), `RateLimitGroup`
(overlapping time windows), `KeyedRateLimitGroup` (per-key quotas), all behind
the `ResourceGroup` interface with optional gauge `describe()`/`read()`.

Groups come in two lifetimes. **Task-scoped** groups are acquired at dispatch and
released at settle. **Resident** groups (`residentGroups`, with a per-worker
cost) are held from worker creation to teardown — for resources a worker holds
across tasks, such as a model loaded in `setup()`. Resident admission is
head-of-line: a waiter that does not fit blocks the queue rather than letting
cheaper pools overtake it.

## Commands

```bash
# Deno
deno task check          # type check (includes scripts/)
deno task test           # all suites
deno task test:events    # one suite: core|error|spawning|settlement|docs|resident|events
deno task examples       # run every example on Deno, then the bundled ones on Node
deno task jsr:smoke      # stage only the publishable files and run a real worker against them
deno lint .
deno task benchmark:quick

# Node (pnpm is the package manager)
pnpm install --frozen-lockfile   # also runs prepare -> the library build
pnpm test
pnpm run test:browser            # Playwright, 3 engines
```

`deno task check` resolves `@types/node` from `package.json` devDependencies, so
it fails with `Could not find "@types/node"` unless dependencies are installed
first. Run `deno install` (or `pnpm install`) before it in a fresh checkout.

## Testing

Seven suites, 132 tests, all in the default `deno task test` run:

| Suite | Tests | Covers |
|---|---|---|
| core | 23 | scheduling, groups, pooling, retention |
| error-handling | 12 | worker failure, restart, requeue |
| spawning | 12 | dynamic spawning, `onCompleteDescendants()` |
| settlement | 17 | exactly-once settlement, counter cardinality |
| resident-groups | 12 | resident admission, weighted costs, affinity |
| resource-events | 23 | `resource.*` events, gauges, admission queue |
| docs | 33 | **executes every snippet in README.md and AGENTS.md** |

`deno task test:scripts` is separate and not included in `deno task test` — it
unit-tests `scripts/publishOutput.ts`.

Two conventions worth keeping:

- **A new documented claim needs a docs test.** The docs suite is what stops the
  documentation drifting from the code.
- **Mutation-check a regression test.** Several tests here were once vacuous —
  a rate-limit test that passed with the limit removed entirely. Break the fix
  and confirm the test fails, then restore it.

## Release process

One tag releases both registries.

```bash
# manifests must already agree; publish refuses otherwise
git tag -a v0.3.1 -m "..." && git push origin v0.3.1
```

`.github/workflows/publish.yml` then runs the version guard, `check`, `lint`,
tests, script tests and `jsr:smoke`, publishes to JSR over OIDC, and — only if
that succeeded — publishes to npm via trusted publishing with provenance.
Neither registry needs a stored token.

`.github/workflows/ci.yml` runs on every push and PR: a Deno job, a Node matrix
(22 and 24), Playwright across three engines, and lint.

Version must be bumped in **both** `deno.json` and `package.json`, which the
publish workflow checks against each other and against the tag.
`pnpm-lock.yaml` does not pin the root version, so it needs nothing.

## Things that will bite you

Each of these cost a real debugging cycle. They are not hypothetical.

**`core/workerWrapper.ts` is not in the module graph.** `ThreadWrapper` reaches
it through `new URL(...)`, a string literal rather than an import. If it were
dropped from the publish allowlist, `deno publish` would report no error and no
warning, the package would publish, and every consumer would fail on their first
threaded task. `deno task jsr:smoke` exists to catch exactly this — it stages
only the publishable files and runs a real threaded worker against them. **A
green dry run is not evidence the package works.**

**`publish.exclude` in `deno.json` is an allowlist** (`**` plus `!` exceptions),
so a new published module must be added explicitly. It currently ships 16 files.

**The package must have no dependencies.** `scripts/` uses `@std/path` and is
kept out of the package by the allowlist alone — Deno has no devDependencies — so
widening the allowlist by one line would make `@std` a real dependency for every
consumer. `jsr:smoke` asserts this too, because `deno publish` only analyses the
graph reachable from `exports` and cannot see it.

**Linux, macOS and Windows are all supported development environments**, and CI
runs ubuntu-latest. No contributor should have to switch platform to work on
this, so nothing may assume a POSIX shell, POSIX paths or a `/`-separated
filesystem. Two classes of bug pass on one platform and fail on another:

- *Path handling.* Never string-strip `file:///`, and never hand-roll
  `^/([A-Za-z]:)` drive-letter regexes. Both are platform-specific by
  construction: stripping `file:///` yields a valid absolute path on Windows and
  eats the leading slash on POSIX, which is a real bug this repo shipped. Use
  `@std/path`'s `fromFileUrl`, `join`, `dirname` and `relative`, and never
  concatenate path separators by hand. Note `Deno.fromFileUrl()` does not
  exist — it was removed in Deno 2.

  Prefer making platform-dependent logic testable rather than testing it on
  every platform: `scripts/publishOutput.ts` takes its path implementation as a
  parameter, so `publishOutput_test.ts` exercises the POSIX branch and the
  Windows branch from whichever platform you happen to be on. That is the
  pattern to copy — a cross-platform bug you can only reproduce by owning the
  other machine is one you will ship.

- *Toolchain drift.* CI pins Deno (`deno-version:` in both workflows) rather
  than floating `v2.x`, because a Deno release once turned CI red with no change
  from us. Keep local Deno in step with the pin, or local runs prove little.
  Node is exercised on 22 and 24 in CI; the package requires >= 22.

**`deno install` rewrites `deno.lock`.** The committed lockfile pins only the JSR
imports, so resolving the npm devDependencies appends a few hundred lines and
leaves the tree dirty — which `deno publish` refuses. `publish.yml` restores that
one path before publishing. Committing a lockfile that pins the npm dependencies
too would retire the workaround.

**pnpm, not npm.** `packageManager` declares it, `pnpm-lock.yaml` is the only
lockfile, and CI installs with `pnpm install --frozen-lockfile`. `pnpm publish`
runs git checks that fail on a detached tag checkout, so the release step itself
uses `npm publish`. There is deliberately no `package-lock.json`: nothing
validated it, so it silently fell a release behind twice before being removed.

**npm errors on republishing an existing version; JSR skips it.** The publish
workflow queries the registry first so re-running a released tag is a no-op on
both.

**`deno fmt --check` fails on ~65 of 68 files.** It is deliberately not in CI —
an always-failing advisory step is noise. Reformatting is a large mechanical
diff; do it as its own change, then add the gate.

## Design documents

`doc/design/` holds plans and analyses: the working notes behind a change that
are too long-form for `TODO_CHECKLIST.md` and too speculative for `AGENTS.md`.

**They are transient, and they are tracked precisely so they can be deleted.**
Once a plan has shipped, remove it. A plan describing what was *going* to happen
is worse than no plan at all — it reads as current, drifts from the code with
every subsequent change, and quietly becomes a maintenance burden nobody
volunteered for. Deleting it costs nothing here, because git history keeps it:
`git log -- doc/design/<name>.md` recovers the reasoning whenever someone asks
why a thing was built the way it was.

That is the whole argument for tracking them rather than keeping them local.
An untracked plan cannot be safely deleted, so it never is, so it rots — and,
as this project learned the hard way, it is also the kind of file that does not
survive losing a working copy.

Two states, and only two:

- **Open** — the work has not shipped. The document is live and should be kept
  current, or explicitly marked abandoned.
- **Implemented** — the work has shipped. Delete the document in the same change
  that closes the work out, or in the release commit.

If a plan contains something with lasting value — a contract, a gotcha, a
measured performance characteristic — that part does not belong in a plan at
all. Move it to `AGENTS.md`, `CLAUDE.md` or a test *before* deleting the plan.
The plan is scaffolding; the durable conclusions are the building.

## Task management

`TODO_CHECKLIST.md` holds active tasks, known issues, the three-phase task
lifecycle (Refinement → Implementation & Verification → Completion) and the
benchmarking process, including the confidence ranges that decide whether a
change is a regression.

**Benchmarks are position-dependent.** A scenario's throughput depends strongly
on where it sits in the suite — earlier scenarios leave heap pressure behind, and
the same commit can measure 40–70% differently in a full suite versus alone.
Compare full-suite runs against full-suite runs, and benchmark two commits back
to back rather than against a stored baseline.
