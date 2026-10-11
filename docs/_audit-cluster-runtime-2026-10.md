# KaimonSlate cell runtime audit: feat/hpc-cluster

All locations below refer to the worktree at `/Users/kburke/devel/KaimonSlate.jl/.claude/worktrees/feat+batch-fabric`, HEAD `b516bb55`. That commit is one past the reviewed `375e1a9e` and only touches region-sweep logging. Each finding has a status:
- **confirmed**: a second reviewer traced it.
- **checked**: I read the code for this report.
- **reviewer-traced**: one reviewer traced it and nobody has independently verified it yet.

**Short answer on "volatile":** that chip on an `@sweep` job cell changes nothing about the cell, its caching, or its readers. It shows up because the ssh helpers the sweep uses (`Sweep.connected`, `run_there`) label any cell that calls them as "reads live state". That label is meant for small CODE cells that ask a host something. It should not appear on job cells. Section 6 has the full answer.

---

## 1. How the cell runtime works today

### States and who owns them

`@enum CellState FRESH STALE RUNNING ERRORED BLOCKED` (engine.jl:78). That enum is only part of a cell's state. The rest is spread across:

- **Cell fields**: `blocked/blocked_at/blocked_host/blocked_region` (engine.jl:177-187; documented as "" whenever state != BLOCKED), `rev` (engine.jl:172), and `flags`. The flags mix author tags with runtime classifications (`:volatile_declared`, `:everywhere_declared`, `:import_scaffold`, ..., listed in `_INTERNAL_FLAGS`, engine.jl:365).
- **Hub globals keyed by (nb.id, cell.id)**: `_DIRTY_WHILE_RUNNING` (server.jl:949), `_FORCE_RUN`, `_PASSED_ON`, `_REGION_PREPARING`, `_RUN_SINCE/_RUN_ORPHAN_HITS`, `_READER_REARMED`, `_REARM_AT/_REPLACE_AT`, `_STOP_ASKED`, `_PARALLEL_CANCEL`.
- **Locked cells**: not a state. A locked cell is FRESH plus the `:locked` flag plus `lockedkey=`/`frozenat=` tags. When there is nothing to restore it is held as BLOCKED with reason `WAIT_LOCKED` (`_hold_locked!`, server.jl:4560).

Ownership:

| Fact | Owner | Notes |
|---|---|---|
| `Cell.state`, `blocked*`, `rev`, `flags` | hub, under `nb.lock` | About 8 sites write `c.state = STALE` directly, bypassing the helpers (deps.jl:825, 837; server_agentops.jl:887, 1222; server_history.jl:240, 1857; server_complete.jl:3209; server.jl:2597) |
| What is actually executing | worker: `_RUNNING_TASKS[cid]`, `_CELL_STOPS[cid]` (worker.jl:1042-1052, capture.jl:1025) | Keyed by cell id with no run token. The hub asks only from the liveness sweep (`__slate_running`) |
| Namespace contents | worker | Hub tracks generation only for main (`_MAIN_GEN`, checked once per runner loop) |
| "Running" on the page | browser: `liveStates['running']` + runstatus.js private `running` Map | `cellrun:` carries only an id (server.jl:893) |
| Sweep progress, settle | durable store markers + the card's JS poll | The browser card is what tells the hub a sweep settled (sweep.jl:4851) |
| Worker liveness | about 14 independent timers/TTLs plus about a dozen hub Dicts | No per-worker record |

Helpers (deps.jl:811-928):
- `restale!` sets any state to STALE, except a locked FRESH cell. It unblocks and bumps rev. It **overwrites RUNNING** (checked, deps.jl:811-817).
- `mark_running!`, `mark_result!`, `mark_errored!` unblock and bump rev.
- `revert_running!` (RUNNING to STALE) and `reset_all!` (everything to STALE, output dropped) do neither (checked, deps.jl:822-839).

### Kind, side, and effect class

- **Side.** `_cell_region` (server.jl:1603) reads the raw `region=` tag and returns "" for JOB cells, because their `region=` names where sweep units run. `_cell_side` (server.jl:1625) adds "an untagged mutator follows its writer's region". About 20 sites still call `_cell_region`.
- **Effect class.** `_cell_effect` (deps.jl:958-990, checked) is the classifier:
  - non-CODE kinds return PURE immediately;
  - then the `resource` tag gives RESOURCE;
  - the `volatile` tag or `:volatile_declared` gives VOLATILE;
  - `everywhere` or `:everywhere_declared` gives EVERYWHERE;
  - a region pin gives PURE;
  - pure-`using`, import scaffold or theme setter gives EVERYWHERE;
  - everything else is PURE.
  - IMPURE is never returned.
- **Region policy per class:**
  - PURE and VOLATILE values are *shipped* by `_region_presync!` (server.jl:4240).
  - EVERYWHERE is *replayed* by `_prime_namespace!` (server.jl:2457), keyed on a hash of source hashes.
  - RESOURCE is *replayed at read time* by `_ensure_resource_on!` (server.jl:4186).
  - Pure `using` cells are *also* re-run by an inline mirror in `_eval_one!` (server.jl:4806-4825).
- **Memo policy.** `_memoizable` (eval.jl:1049) is CODE-only and goes through `_cell_effect`. `_key_poisoned` (eval.jl:1234-1242, checked) re-reads raw flags (`:nocache`, `:volatile`, `:opaque`, `:resource`) and does not consult the classifier.

### How runs start, stop and restart

- **Start.** Every entry point (▶, run-all, edit, `@bind` push, reactive write, restart, region grant) only restales cells and kicks `_ensure_runner!`. One runner per notebook (`_RUNNERS`, server.jl:5402) calls `_reestablish_fresh_namespace!` once, then loops. Each pass either:
  - runs a hub-side parallel batch (`_run_code_batch!`, server.jl:5096; skipped whenever any region is active), or
  - takes the first STALE cell in document order (`_next_stale_cell`, server.jl:1010) and runs `_eval_one!`.
- **A region cell's path:**
  1. `_prepare_region_for_cell!` marks it RUNNING before any worker exists (server.jl:4497).
  2. Placement / bring-up, possibly BLOCKED with a wait reason.
  3. `_prime_namespace!`.
  4. `_region_presync!`.
  5. RUNNING again at dispatch (4784).
  6. `eval_capture`.
  7. Merge: re-resolve the cell by id, compare `src_hash`, consult `_DIRTY_WHILE_RUNNING`, then `mark_result!` (4828-4846). The failure exits in between write to the *captured* Cell object instead.
- **Stop** (`cancel_run!`, server_complete.jl:115-136):
  - sets `_PARALLEL_CANCEL`;
  - finds targets by `c.kind == CODE && c.state == RUNNING`;
  - sends one cancel per kernel (30 s timeout each);
  - if nothing answers, falls back to `restart_kernel!`, which kills main and every region.
  - On the worker, a stop is a per-cell flag checked at `slate_checkpoint`/progress/log. The cell comes back as an ordinary ERRORED result.
- **Other cancellation mechanisms.** Preemption on edit (`_preempt_superseded!`, server.jl:5034) is a second one. `@onclick` handlers have a third, a `Ref{Bool}` token (reactive.jl:95-226).
- **Restart** (`restart_kernel!`, server_complete.jl:193-238): interrupt, shutdown, kill regions, `reset!` (every cell STALE), then `_drain!`. `_drain!` reuses the existing runner loop, so `_reestablish_fresh_namespace!` does not run again.
- **Self-healing.** Four periodic reconcilers in the 5 s supervisor correct the cell states when they are wrong (server.jl:3725-3769): `_reconcile_nb_runs!` (orphan healer), `_reconcile_stale_runner!` (wedge detector), `_reconcile_blocked_regions!`, `_reconcile_stranded_readers!`.

---

## 2. Where the semantics are incoherent

The repeating pattern: **a fact that should come from comparing an identity is instead recorded as a side effect at each event site that is known to change it.** Every event site that gets forgotten becomes a bug, and the fix adds one more site. The git history shows these chains directly:

| Chain | Commits |
|---|---|
| Restale on worker replacement | 75cf5d19, 6c6a0fe9, f21d864b, 2b83f299, a7ffc46c, 6e39df77 |
| Worker reuse validity | 858fdadf, 753874d6, 7a07e765, a87e69bf, e7a9f09e, e1e0b214, a372f30a, 255d8f50, 88614f03, aade3ed3, 6691037a |
| Who submits or advances a sweep | 8c758826, a51e6e2a, fd80057d, 7abba4c9, 758715ec, 18454402, 681f2c17, 13be567b, bb7082e3, fa235f0d, 375e1a9e |
| Env stamp / compile-mark staleness | 6769c1d9, f80839e7 (regression), 251803f0, b66cba29, 6376a74a, 48dd2239 |
| Node keep / release / withdraw intent | 943ac36a, d20c6df9, c03fdd59, ae070f91, 5d4ee6fe, e68269be, 138fef9b, 8dd1f3ff |

Below, each incoherence is paired with the concept that is missing.

### A. `Cell.state` means both "needs a run" and "is executing". Missing concept: an in-flight Run record.

`restale!` overwrites RUNNING with STALE while the worker keeps computing. Everything that reads `state == RUNNING` as "in flight" is then wrong:

- **Lost restale (checked).**
  - Only `_reactive_refresh!` notes its *seed* cells in `_DIRTY_WHILE_RUNNING` (server.jl:798).
  - These paths restale a running cell without the note: the dependents it restales a few lines later, `update_source!`'s dependents cascade (deps.jl:1218-1224), `server_src_changed`, `_restale_side!`, the pkg-op wipe and rerun-all.
  - The merge then marks the stale-input result FRESH.
  - 967f2ff4 fixed this for one caller only.
  - Dirty entries are also left behind on error/blocked exits, which causes a spurious re-run later.
- **Stop and preemption cannot see the cell (confirmed).** `_running_kernels`, `_preempt_victims`, `_stop_pending` and `_interrupt_inflight!` all select on `RUNNING`.
- **Wedge detector.** It sees "STALE pending and nothing RUNNING" and, after 600 s, starts a second runner while the first is still inside `_eval_one!` (server.jl:3120-3155). The first runner's `finally` then deletes the second runner's flag.
- **Region busy.** `_regions_in_use` (3477-3479) stops counting a region that is still computing.
- **Stale Cell object (reviewer-traced; the object replacement is checked at deps.jl:1175-1191).** `update_source!` replaces every Cell object on any edit. `_eval_one_run!` writes failure transitions to the object it captured, so a region cell can stay RUNNING forever in the report. `_REGION_PREPARING` hides it from the orphan healer (server.jl:3100). It is never cleared on prepare failure.
- **Reconnect re-render (confirmed).**
  - `server_celldone(nb, "reconnect", ...)` calls `mark_result!` whatever the state. Its guard reads `_BATCH_SNAPS`, which nothing writes (server.jl:4983, 5206).
  - A STALE edited cell becomes FRESH with its pre-edit output. A running cell becomes FRESH.
  - On the worker, the re-render re-runs the whole cell source under the real cell id, steals and then deletes `_CELL_STOPS[cid]` (capture.jl:1025, 1041), and fires the cell's cleanups.
- **Results after restart (confirmed).** The merge checks only `src_hash`. A dying cell's error can land after `reset!` and leave it ERRORED. The runner keeps going across the restart without re-establishing the namespace, so binds are not re-seeded and `_MAIN_GEN` keeps the old key. The next drain therefore re-runs the whole notebook a second time.
- **Browser (reviewer-traced).** RUNNING reaches the page only through `liveStates`, which every publish wipes (store.js:71), and runstatus's `running` Map, which is never reconciled. The orphan healer reverts a cell without sending a celldone, so the topbar shows "Running" indefinitely.

All of these are one missing object: a hub-side **Run** `(run_id, cell_id, src_hash, side, worker_gen)`. Its presence would mean "executing", and only it could accept a result.

### B. The namespace is invalidated by event, not by generation. Missing concept: a per-side worker generation that each FRESH cell is tied to.

"The worker's namespace was replaced, so its cells are invalid" has several implementations, each with a different scope and locked-cell policy:

- `reset_all!` inside `GateKernel.prepare!` for a local respawn (gate_kernel.jl:1374). It wipes *every* cell, region cells included, runs off `nb.lock`, and does not bump rev.
- `_restale_side!` (server.jl:2589-2601). CODE cells only, bypasses the lock guard. It is called from five event sites: prepare-replace at 1315, grant at 2285, reap at 3012, roster restart at 3067, `restart_region!` at 2570.
- `_reestablish_fresh_namespace!` (server.jl:5239-5288). All runnable kinds, goes through `restale!`, so locked FRESH cells stay FRESH in a blank namespace. Main kernel only, once per runner loop.
- The region prime and sync dedup keys fold in `ns_gen`. That re-ships values *into* a region but never restales values computed *on* it.

Consequences (confirmed):
- A region worker replaced inside `_spawn_and_connect_remote!` (OOM, stale payload, env rebuilt, reaped as wedged: remote.jl:3213, 3223-3237, 3331-3344) bumps `ns_gen` and restales nothing. Same-side readers fail with `UndefVarError` while their upstream shows FRESH.
- A remote main worker that respawns mid-drain runs the rest of that drain on a blank namespace.
- Only main gets its `@bind` registry re-seeded. A region-hosted slider resets to its widget default on a new node (reviewer-traced).
- "Was this a new allocation" is decided by `timeleft < walltime - 120` (server.jl:2280). That started as a log-wording choice (1d87d194) and was then promoted to gate `_restale_side!` (2b83f299).

### C. Stopping is not a hub concept. Missing concept: a per-notebook run-control state and a STOPPED cell state.

- **Stop does not stop a serial run (confirmed).** `_PARALLEL_CANCEL` is read only inside `_run_code_batch!`, which never runs when a region is active. The serial loop (server.jl:5320-5342) checks only `_RUNNER_CANCEL` (notebook close). After the stopped cell merges as ERRORED, the runner runs the next STALE cell. Downstream cells run against the stopped cell's *previous* values, because a graceful stop keeps the namespace, and they go FRESH. `_run_code_batch!` also resets the flag at batch start (5109), erasing a Stop pressed during bring-up.
- **JOB/WEB/TOOL cells are invisible to Stop and preemption.** `_running_kernels` filters `kind == CODE` (server_complete.jl:168). The same narrowing was already fixed once for parallel batching (server.jl:5054-5058).
- **Three cancellation mechanisms (confirmed).** Cell stop flags, preemption (shares `_stop_cells!`), and the `@onclick` token. `pause` ignores the cell flag and `slate_checkpoint` ignores the handler token. The app-mode allowlist justifies `/cancel` with "`@onclick` bodies are cancellable", which `/cancel` does not do (server_app.jl:59).
- **Escalation is too broad (confirmed).** An unresponsive region costs up to 30 s per kernel on the HTTP request, then a restart of *everything*. A stop aimed at a not-yet-connected kernel counts as delivered and is then lost.
- **Preemption round-trips to the worker under `nb.lock` (confirmed).** Up to 5 s per region kernel (server.jl:5034-5049). That breaks the invariant at server.jl:100-117, and `cancel_cells` is missing from `_KERNEL_BOUNDARY`.
- **Dead batch protocol still wired in (confirmed).** `__slate_eval_batch`, `eval_batch`, `_BATCH_CANCEL`, `_interrupted_wire`, `_BATCH_SNAPS`.
- **Edits and value changes have opposite policies.** An edit preempts. A `@bind`/reactive change lets the old run finish and re-runs it via the dirty set.

### D. Runtime classifications are stored as author-tag bits. Missing concept: declared effects as a derived record per (cell, src_hash).

- **Any edit drops them from every cell (checked).** `update_source!` carries over only `output/state/deps/binds/interp` (deps.jl:1181-1186). `:volatile_declared` and `:everywhere_declared` are never in the header, and `_reestablish_effects!` runs only at open (server.jl:557). After any edit:
  - a registrar cell stops being primed on new region workers;
  - a `Sweep.connected` cell becomes memoizable again, which is the stale-padlock bug 26632d1e set out to fix;
  - the carried `output.effects` still shows the badge, so the UI says one thing and the behaviour does another.
- **The tag editor wipes them.** `set_cell_tags!` keeps only `:opaque` (server_agentops.jl:1281). Meanwhile `cell_json` ships every flag except `:opaque` as `tags` (server_history.jl:1110, checked), so `volatile_declared` appears as a removable "Custom" chip. The browser round-trips it; the agent path drops it. There are three hand-kept lists of "internal": `_INTERNAL_FLAGS`, `cell_json`, `set_cell_tags!`.
- **Lifecycle has no single rule.** In memory, flags are add-only (`_apply_cell_effects!` returns early on empty effects, server.jl:1704). On disk, the last run that declared anything wins, and the record is never deleted: EffectStore's "empty deletes" branch is unreachable.
- **"One classifier" is not upheld.**
  - `_key_poisoned` reads raw flags and ignores `:volatile_declared` (eval.jl:1238, confirmed). Downstream readers of a host-querying CODE cell restore stale results.
  - Non-CODE kinds bypass the classifier entirely (deps.jl:960), so `resource`/`volatile` tags on JOB/TOOL cells silently do nothing.
  - IMPURE is dead.
- **Everywhere replay is unfiltered.** It re-executes the statement of *every* declared record, not just `:everywhere` ones (server.jl:2524-2542). A `Sweep.connected` or `pick_on!` statement is then replayed on every region prime (reviewer-traced).
- **Sweeps declare two contradictory things.** `:value_identity` says "tracked identity". `:volatile` says "untrackable". The JOB case works only because every consumer happens to ignore `volatile` for JOB. Fixing `_key_poisoned` the obvious way (adding `:volatile_declared` to the flag test) would poison every sweep reader.

### E. Cross-side value movement uses different freshness rules per mechanism. Missing concept: one value-identity token.

All reviewer-traced:
- **EVERYWHERE prime and RESOURCE replay are keyed on source only** (server.jl:2442-2445, 4188). A theme cell driven by a slider, or a resource opened from a bind-chosen path, never refreshes on the region.
- **The presync token omits reactive and value-identity digests.** It uses writer `src_hash:objectid(output)` plus bind values (4302-4311), but not `_STATE_DIGESTS`. A reactive write is not re-shipped, even though the memo key (eval.jl:1330-1345) does move.
- **Failures are recorded as success.** `eval_capture` never throws, so `_prime_namespace!`'s `try/catch` fallback is dead. A failed prime is recorded as primed (2507) and a failed resource open as opened (4217-4221).
- **"Which writer supplies this read" is resolved four times** by first-other-side-writer in document order, each with its own "namespace, not data" filter (4194, 4247, 4268, 1625). None uses the dependency graph's last-prior-writer.
- **The `using` mirror duplicates the prime** (4806-4825). It is unkeyed, bypasses hold policy, and can request allocations for every region named in the notebook.

### F. Worker liveness and identity are scattered. Missing concept: one WorkerRecord with a lifecycle and one liveness oracle.

- **The attach ladder reaps busy workers (confirmed).** Every rung reads gate silence as "wedged" and reaps (remote.jl:2929-2963, 3265-3343). The premise is that the gate answers on the interactive thread. KaimonGate actually serves on the default pool (gate_protocol.jl:767), and server.jl:2696 already says so and uses telemetry as the fallback.
- **A dropped wire abandons the run only on the hub (confirmed).** The worker keeps computing. Reattach never reconciles, so a re-run dispatches the same cell id concurrently. The `_RUNNING_TASKS[cid]` delete has no identity check, so the healer then reverts the live run (worker.jl:1042-1052).
- **Two kill paths (confirmed).**
  - Restart uses `teardown_remote!(kill=true)`: SIGTERM only, no children, deletes the manifest before the process is confirmed gone, skips reap mark, conn, claim and peer-route eviction.
  - The roster uses `reap_remote_worker`: TERM then KILL, full eviction.
  - Its "Manual only" docstring is contradicted by automatic calls.
- **`_REAPED` is keyed by host alias.** It is marked under the login host and the node, but unmarked only under the node (remote.jl:3215 vs 6497-6499). A reused port is then treated as "being reaped" for 10 min and a live worker is orphaned (confirmed).
- **The spin guard self-exits detached workers that have background CPU work.** Only `__slate_running` refreshes the heartbeat, and detached workers are never pinged (worker.jl:3250).
- **Liveness pings run serially inside the single supervisor.** It takes `k.lock` inline in `_heal_dead_wire!` (plausible).
- **About 14 separate time-based judgements.** Telemetry "fresh" alone has four thresholds (10, 10, 5, 20 s). `_DEAD_WIRE_GRACE` is a `const` reading ENV, so it is frozen at precompile (server.jl:2672).

### G. Allocation lease. Missing concept: a per-region lease state machine fed by tri-state scheduler answers.

All confirmed unless noted:
- **An unreachable roster reads as "no workers" and triggers release** (remote.jl:5921, 5944, 6240-6246).
- **Idle release decides at tick time and acts later**, reaping attached workers without re-checking (server.jl:3561-3583, remote.jl:5798).
- **`region_release!` forgets the placement before scancel confirms.** The idle path announces "released" regardless of the result.
- **Closing a notebook releases its node within minutes even with `idle_release = 0`.** Reconcile trims the detached worker, then releases. Reconcile also calls `region_place!(submit=true)`.
- **The SLURM find is not user-scoped** (`squeue -h -n`, allocation.jl:169-170). PBS, the ETA probe and the occupancy probe all are.
- **Placement-drop cleanup is copied in three places.** `region_place!`'s copy leaks the file syncer and data tunnels (remote.jl:5669-5672).
- **Check-then-submit with no per-region serialization** outside `_place_in_background!` (plausible).
- Intent is tracked by wall-clock stamps (`_WITHDRAWN_AT`, `_RUN_ASKED_AT`, `_GAVE_UP` 120 s). A tag toggle re-arms placement for a withdrawn region (plausible).

### H. Sweep advancement. Missing concept: one owner per started run.

Reviewer-traced unless noted:
- **The card poll submits from a cached `collect_jobs` answer** that its own submission never invalidates (sweep.jl:3607-3609, clustercollect.jl:98-103). The same chunk is resubmitted every poll and the retry budget runs out.
- **A failed poll returns `:unknown` for every name, and `plan` treats that as not live.** Running work is resubmitted. PBS `qselect ... 2>/dev/null || exit 0` turns a server outage into a successful empty answer. `cancel!` cancels nothing and still writes `.cancelled`.
- **Two advancers in two processes (hub supervisor, card poll) with no lock** around plan, index, submit and `bump_attempts!` (plausible; the cell run submits only with an explicit `submit=true`).
- **Programmatic `reset!(r)` has drifted from the card's reset.** It runs no cancel first and deletes the index files that name the live jobs.
- **Settle reaches readers only if a browser card watched it happen** (sweep.jl:4851-4853, 3445-3452), even though `advance_started!` already computes `is_settled`.

### I. Transport failures travel as booleans and message text. Missing concept: typed result kinds.

All confirmed:
- **A read error mid-command is reported as success** (sshtransport.jl:446-457). Truncated `squeue`/`sbatch` output comes back with `ok=true`.
- **A channel-open timeout marks the whole session dead.** The `run_there` retry then re-runs a command that may already have run, so `sbatch` can submit twice (remotestore.jl:271-279).
- **`_channel_dead!` leaks the session**: owner task, fd and listeners that accept connections they never serve.
- **`_reach` opens a channel without `openlk`.**
- **Teardown frees the libssh2 session while open loops still call into it** (NULL dereference).
- **Control flow depends on message text**: backoff class, CURVE retry, "Already connected". The CURVE fetch falls back to dialling *without a key*.
- **Timeouts do not cover the semaphore wait or the channel open.**

### J. Hub to browser. Missing concept: a per-cell, rev-gated merge and gap detection.

Reviewer-traced:
- A full-state publish replaces a newer pushed cell with an older one; only the output DOM is rev-gated.
- On backpressure overflow the server sends a version token that the client ignores, because the version did not change.
- The worker dot reads `state.worker` while the worker pills read facts.
- Timing-based pulls (250 ms converge, 2 s ack gap, 2 s blocked sweep) stand in for missing state.

### K. Provisioning identity. Missing concept: one EnvIdentity value.

Reviewer-traced except the confirmed suffix chain:
- **The payload SHA is folded into the env fingerprint** (remote.jl:1096). Any worker-code edit forces a full rebuild everywhere and gates cells with the false reason "package versions changed".
- **`stamp_env_precompiled!` recomputes the stamp from current local files** after an hours-long compile.
- **"Compiled" is recorded twice** (`+pc` and the `.pc` mark), and the start only reads one.
- **`_julia_sh` overrides `Machine.julia` for builds**, but not for launches.
- **Site stamps are per host but read from one region's node type**, and staleness is a sentence parsed back by regex.

---

## 3. A proposed coherent model

Ten rules. Each replaces a set of special cases.

**R1. Cell state is only the scheduling verdict.** `{STALE, FRESH, ERRORED, STOPPED, BLOCKED(reason)}`.
- The field is private.
- Every change goes through one `transition!(c, to; reason)` that applies the lock guard, unblocks, and bumps rev.

**R2. Execution is a Run record owned by the runner.** `Run(id, cell_id, src_hash, side, worker_gen)` in `nb.runs`.
- "Running" means a Run exists for the cell. The UI, Stop, busy-region, the wedge detector and the healer all read `nb.runs`.
- `restale!` on a cell with a Run sets `run.rerun = true` and leaves the Run in place.
- `finish!(nb, run_id, out)` is the *only* merge. It accepts a result only if the run is current (same run id, same worker generation, same src_hash). It then honours `rerun`, and it clears the run on every exit path, including error and blocked.
- Run ids travel to the worker, and the worker keys `_RUNNING_TASKS` and stop flags by run id.

**R3. Each side has a namespace generation.** `gen(side) = _worker_key`. A FRESH cell records the gen it was computed in.
- Before every dispatch to side S: if `gen(S)` differs from the last seen value, call one `invalidate_side!(nb, S)`. That function:
  - restales S's executable cells;
  - marks locked cells "restore pending";
  - re-seeds S's binds;
  - clears S's prime, resource and sync records.
- Main and regions use the same path.

**R4. One run-control per notebook.** `stop_requested_gen` lives under a lock.
- The runner checks it before every step.
- Stopped cells go to STOPPED, which `restale!` and `reset_all!` leave alone; only ▶ clears it.
- Transitive dependents of STOPPED or ERRORED cells are held `BLOCKED(WAIT_UPSTREAM)`.
- One cancellation token type covers cell runs and handler tasks. `slate_checkpoint`, `pause`, progress and Reactive writes all check it.
- Stop is asynchronous, escalates per side, and is remembered for kernels that are not connected yet.

**R5. Executable kinds are one predicate.** `executes(c)` is CODE ∪ JOB ∪ WEB (∪ TOOL where relevant). Every state consumer uses it.

**R6. Author tags and declared effects are separate.**
- Tags come from source.
- `declared[(cell_id, src_hash)]::Set{Symbol}` is *replaced* by each completed run's harvested effects, an empty set included, mirrored to the EffectStore, and loaded at open.
- `_cell_effect` reads tags plus `declared`, and every consumer (memo, poison, prime, transfer, badge) asks `_cell_effect`.
- `value_identity` supersedes `volatile` within a harvest.
- `_INTERNAL_FLAGS` disappears.

**R7. One value-identity token per name.** It is built from the writer's src_hash, its output identity, and the bind/state digests.
- The memo key, presync dedup, prime and resource records all use it.
- A volatile producer publishes a digest of its value, the way sweeps already do, so readers key on it instead of being poisoned.
- One replay helper checks `out.exception` and records success only on success.

**R8. One WorkerRecord per (node, port, boot nonce).**
- Lifecycle: `{starting, live, busy, unresponsive, reaping, gone}`.
- One liveness oracle: telemetry or stats freshness plus process presence decides alive vs dead. Gate silence alone only ever means busy.
- One `kill_worker!` (TERM, grace, KILL including children; delete the manifest only once the process is gone) and one `forget!`.
- After any attach, reconcile the worker's in-flight runs: adopt or cancel them.

**R9. One lease state machine per region.**
- States: `{none, requesting(gen), held(job_id), releasing, unknown}`.
- Scheduler answers are tri-state: present, absent, unreachable.
- Release requires positive evidence of emptiness and cancels by job id.
- Forget the placement only after the scheduler confirms. Release stays pending and is retried otherwise.
- Holders are explicit: open notebooks, prepares, pins.

**R10. One owner per started sweep, and typed transport results.**
- The hub supervisor owns each started run. The card polls only observe or kick it.
- The supervisor emits settle and partial-landing digests to readers.
- Polls are tri-state. Transport calls return `{ok, exit_nonzero, not_started, lost_mid_run, timed_out, auth_needs_person}`, and retry only `not_started`.

What existing special cases become:

| Today | Under the model |
|---|---|
| `_DIRTY_WHILE_RUNNING`, caller-side dirty notes | `run.rerun` set inside `restale!` (R2) |
| `_BATCH_SNAPS`, `server_celldone` state path for reconnect | reconnect swaps only the live output chunk; no state change (R2) |
| captured Cell object in `_eval_one_run!`, `_REGION_PREPARING` | transitions via `with_cell(nb, id, run_id)`; preparing is a Run in phase `:preparing` (R2) |
| orphan healer, wedge detector hit counters | compare `nb.runs` with the worker's run-id set (R2, R8) |
| `liveStates['running']`, runstatus Map | derived from cells whose Run exists, pushed with rev (R2) |
| `reset_all!` in `prepare!`, five `_restale_side!` sites, `_MAIN_GEN`, `found` heuristic | `invalidate_side!` on generation mismatch (R3) |
| main-only bind re-seed | part of `invalidate_side!` (R3) |
| `_PARALLEL_CANCEL`, `_STOP_ASKED`, `_BATCH_CANCEL`, `_interrupted_wire`, handler token | run-control plus one token type (R4) |
| ~28 `kind == CODE` state checks | `executes(c)` (R5) |
| `:volatile_declared`/`:everywhere_declared` flags, three internal-flag lists, `set_cell_tags!` special case | `declared[(id, src_hash)]` (R6) |
| `_key_poisoned` raw flag test | `_cell_effect` plus value identity (R6, R7) |
| `_prime_env` source hash, `_REGION_RESOURCED` src_hash key, presync token | value-identity token (R7) |
| `using` mirror in `_eval_one!` | deleted; prime covers it (R7) |
| `_REAPED` TTL, `_KERNEL_UNRESPONSIVE_SINCE`, `_KERNEL_BUSY_SINCE`, park/record validity guesses | WorkerRecord state (R8) |
| `teardown_remote!(kill)` vs `reap_remote_worker` | `kill_worker!` (R8) |
| `_GAVE_UP`, `_GRANT_GRACE_S`, `_WITHDRAWN_AT`, `_RUN_ASKED_AT`, `_region_serves_kernel` | lease states (R9) |
| card-poll `submit=true`, browser `settled` action | supervisor-owned advance and settle (R10) |
| `occursin("declined or cancelled")`, `"Unable to create step"`, `"no slate_on handler"` | typed kinds (R10) |

---

## 4. Defects to fix regardless

### High

| # | Defect | Location | Failure | Status |
|---|---|---|---|---|
| 1 | Stop does not stop a serial run | server_complete.jl:115-136; server.jl:5101, 5109, 5320-5342 | Region notebook, Stop on cell 3 of 20: cells 4-20 still run; downstream goes FRESH on old values | confirmed |
| 2 | A restale of a RUNNING cell is lost | deps.jl:811-817, 1218-1224; server.jl:798, 4842 | Edit A while its reader B runs: B goes FRESH with the old A value and never re-runs | checked |
| 3 | Reconnect re-render marks cells FRESH and steals stop flags | server_sse_import.jl:78-89; server.jl:5200-5229; capture.jl:1025, 1041, 1230-1247 | Reload while an edited live cell is queued: the edit never runs; a running cell becomes unstoppable and is re-executed concurrently | confirmed |
| 4 | In-flight results merge across restart; restart reuses the runner loop | server.jl:4828-4845, 5299, 5405; server_complete.jl:193-238 | Dying cell lands ERRORED after reset; binds not re-seeded; second full re-run on the next edit | confirmed |
| 5 | Failure transitions written to a replaced Cell object | server.jl:4495-4540, 4721-4771; deps.jl:1175-1191 | Edit during a 90 s region bring-up, then allocation lost: report cell stuck RUNNING, node never released | reviewer-traced (replacement checked) |
| 6 | Any edit drops `:everywhere_declared`/`:volatile_declared` from every cell | deps.jl:1181-1186; server.jl:557 | Registrar cell stops priming new region workers, giving UndefVarError; `connected` cell restores a stale answer | checked |
| 7 | Attach ladder reaps a busy worker | remote.jl:2929-2963, 3265-3343; gate_protocol.jl:767 | `Threads.@threads` cell plus session blip: worker SIGKILLed mid-computation | confirmed |
| 8 | Dropped wire leads to concurrent same-cid evals | worker.jl:1042-1052; remote.jl:3223-3251; server.jl:3074-3112 | Partition longer than 45 s, re-run: two evals mutate the same globals, healer reverts the live one | confirmed |
| 9 | Region worker replaced in place leaves cells FRESH | remote.jl:3213, 3223-3237, 3331-3344; server.jl:4458-4520 | OOM-killed region worker, cold respawn: downstream UndefVarError, upstream shows FRESH | confirmed |
| 10 | Unreachable roster treated as empty, so the node is released | remote.jl:5921, 5944, 6240-6246; server.jl:3697 | Laptop sleeps: next sweep scancels a live job | confirmed |
| 11 | SLURM job lookup not scoped to the user | allocation.jl:169-170 | Two users with region `gpu`: A adopts B's job and never gets a node | confirmed |
| 12 | SSH read error reported as success | sshtransport.jl:446-457, 465-476 | Truncated `squeue`/`sbatch` output with `ok=true` | confirmed |
| 13 | `run_there` retry re-runs a command that may have started | sshtransport.jl:444, 532, 556-571; remotestore.jl:271-279 | Slow login node: `sbatch` array submitted twice | confirmed |
| 14 | Card poll submits from a cached scheduler answer | sweep.jl:3607-3609; clustercollect.jl:30, 98-103; batchsweep.jl:593-700 | Probe chunk resubmitted every 2 s, retry budget spent | reviewer-traced |
| 15 | Failed scheduler poll means "nothing live" | batchlauncher.jl:872, 1096, 1579, 1599; batchsweep.jl:596 | pbs_server restart: every running chunk resubmitted; Cancel cancels nothing but shows "cancelled" | reviewer-traced |
| 16 | Prime and resource failures recorded as success | gate_kernel.jl:1452-1469; server.jl:2484-2507, 4216-4221 | A failed resource open never retries for the life of the worker | reviewer-traced |
| 17 | Prime and resource keyed on source only | server.jl:2442-2445, 4175, 4188 | Slider-driven theme or bind-chosen DB stays old on the region | reviewer-traced |
| 18 | Presync token ignores reactive digests | server.jl:4302-4311 | Region reader of a `reactive` sees the old value | reviewer-traced |
| 19 | Payload SHA in the env fingerprint | remote.jl:266, 1096; server.jl:1208-1214 | Any worker.jl edit triggers a full rebuild on every host and a false "packages changed" prepare gate | reviewer-traced |
| 20 | Browser RUNNING mark wiped by any push; run Map never reconciled | store.js:68-72; runstatus.js:288-300; server.jl:890-895 | "Running k/N" pill forever after an orphan heal | reviewer-traced |
| 21 | Sweep settle reaches readers only via a watching browser card | sweep.jl:3445-3453, 4851-4853, 1556 | Agent-driven or closed tab: readers stay on the partial landing indefinitely | reviewer-traced |

### Medium (abbreviated)

- **Run control**
  - Preemption round-trips under `nb.lock` (server.jl:5034-5049; gate_kernel.jl:1938-1950). Confirmed.
  - Stop takes 30 s per kernel, then restarts every region (server_complete.jl:115-141). Confirmed.
  - JOB/WEB/TOOL cells are invisible to Stop and preemption (server_complete.jl:168; server.jl:5031, 3937).
  - BLOCKED reason and region are dropped on edit, so `_reconcile_blocked_regions!` never resumes the wait (deps.jl:1182-1186; server.jl:3200).
- **Memo and effects**
  - `_key_poisoned` ignores `:volatile_declared` (eval.jl:1238). Confirmed. The fix is to route through `_cell_effect`, which keeps sweep readers keyable because JOB is PURE.
  - Everywhere replay includes non-`:everywhere` statements (server.jl:2524-2542).
  - Declared-effect lifecycle: add-only in memory, never deleted on disk (server.jl:1704, 1737; effectstore.jl:21-31).
  - Internal flags shipped as tags and dropped by agent tag edits (server_history.jl:1110; server_agentops.jl:1281).
- **Region boundary**
  - Region binds are not re-seeded on a new worker (server.jl:5232-5287).
  - The `using` mirror can request allocations (server.jl:4806-4825).
  - First-writer scans conflict with the dependency graph (server.jl:4194, 4247, 4268, 1625).
- **Workers**
  - Restart kills with SIGTERM only and deletes the manifest first (remote.jl:3474-3500 vs 6496-6549). Confirmed.
  - `_REAPED` host-alias mismatch (remote.jl:3215, 6483-6500). Confirmed, low frequency.
  - The spin guard kills detached workers doing background work (worker.jl:3243-3255). Confirmed.
  - `_DEAD_WIRE_GRACE` reads ENV in a `const` (server.jl:2672).
  - Hub-global bring-up narration masks a wedged runner in another notebook (server.jl:3124-3160).
  - `slate_call` matches the substring "no slate_on handler", which stops a live card's polling (server_complete.jl:3582-3634; sweep.jl:4886-4890).
- **Allocations**
  - Idle release race (server.jl:3561-3583). Confirmed.
  - Placement forgotten before scancel confirms; "released" announced regardless (remote.jl:5798-5808). Confirmed.
  - Close leads to release despite `idle_release = 0` (server_complete.jl:4413; remote.jl:5944-5990). Confirmed.
  - `region_place!` placement drop leaks syncer and tunnels (remote.jl:5669-5672). Confirmed.
- **Transport**
  - `_channel_dead!` leak (sshtransport.jl:556-571). Confirmed.
  - `_reach` without `openlk` (641-649). Confirmed.
  - NULL session dereference on teardown (910-921, 995-1004). Confirmed; a hub crash, though narrow.
  - ProxyJump ignored and no connect timeout (42, 62-80). Confirmed.
  - `exec_io` hard 300 s and truncated EOF (587-625). Confirmed.
  - Timeout excludes slot wait (955-961). Confirmed.
  - Peer blob stall-resume branch unreachable, plus leaked tmp files (blobchannel.jl:241-286). Confirmed.
- **Sweeps**
  - Two advancers without a lock (sweep.jl:1556, 3608; batchsweep.jl:676-715). Plausible.
  - `reset!(r)` orphans live jobs (sweep.jl:2975-3008).
  - `push_meta!` sends the whole mirror (remotestore.jl:1122-1150).
  - `.pullstamp` shared across mirrors and mtime-0 archives (remotestore.jl:1080-1096).
- **Provisioning**
  - Stamp recomputed after compile (region_prepare.jl:647-664).
  - `+pc` vs `.pc` (remote.jl:1197-1203).
  - `_julia_sh` overrides `Machine.julia` (remote.jl:599-621).
  - Site stamps per host (region_prepare.jl:346-362).
- **Browser**
  - Backpressure token ignored (server_sse_import.jl:11-18; panels.js:575-576).
  - Full state not rev-gated (view.js:1787-1816).
  - Worker dot has two sources (server_history.jl:1267; view.js:1829-1848).

---

## 5. Cleanup plan

Largest leverage first. Each step can be tested on its own.

**Step 0. Independent small fixes that cause damage today.** None depends on the structural work.
- Add `-u "$USER"` to the SLURM find, and cancel by job id (#11).
- Make `_drain` return a tri-state and treat a negative read as failure (#12).
- Make `run_there` retry only when the channel open failed (#13).
- Require `(ok, workers)` in `_region_has_live_workers` and do nothing when not ok (#10).
- Have `_key_poisoned` call `_cell_effect(c) == VOLATILE` (keeping the `:resource` exemption).
- Filter `_INTERNAL_FLAGS` out of `cell_json` tags and preserve them in `set_cell_tags!`.

Tests: one unit test per fix. Cluster-facing ones should run through Slate's own functions against the SlateCluster docker cluster.

**Step 1. Run record and a single merge (R1, R2).**
- Add `nb.runs`, `finish!`, and `transition!`; make `state` private.
- Send run ids to the worker and key `_RUNNING_TASKS`/`_CELL_STOPS` by run id with identity-checked deletes.
- Reconnect becomes output-only.
- *Deletes:* `_DIRTY_WHILE_RUNNING`, `_BATCH_SNAPS`, the state path of `server_celldone`, captured-Cell writes, `_REGION_PREPARING`, `revert_running!`, direct `c.state =` writes, `liveStates['running']`, runstatus's private Map.
- *Tests:*
  - restale during a run triggers a re-run;
  - reconnect during a run or a queued edit leaves state alone;
  - a result arriving after restart is discarded;
  - an edit during bring-up followed by allocation loss does not wedge;
  - Stop finds a cell restaled mid-run.

**Step 2. Per-side generation (R3).**
- `invalidate_side!` is called at dispatch on generation mismatch.
- *Deletes:* `reset_all!` in `GateKernel.prepare!`, the five `_restale_side!` calls, `_MAIN_GEN`'s once-per-loop check, the `found` timeleft heuristic.
- Bind re-seeding becomes per side.
- *Tests:*
  - kill a region worker between runs: same-side cells restale;
  - remote main respawns mid-drain;
  - new allocation grant;
  - a region slider value survives a node change.

**Step 3. Run control and cancellation (R4, R5).**
- STOPPED state, held dependents, one token type, async per-side Stop escalation, preemption moved off the lock, `executes(c)` everywhere.
- *Deletes:* `_PARALLEL_CANCEL`, ad hoc `_STOP_ASKED`, `__slate_eval_batch`, `eval_batch`, `_BATCH_CANCEL`, `_interrupted_wire`, the separate handler cancel path.
- *Tests:*
  - Stop in a region notebook halts the drain;
  - dependents of a stopped cell are held;
  - Stop reaches a JOB cell and an `@onclick` handler;
  - restart does not re-run STOPPED cells.

**Step 4. Declared effects store (R6).**
- `declared[(id, src_hash)]` with replace semantics, consulted by `_cell_effect`.
- Classify every executable kind.
- Filter the everywhere replay to `:everywhere` records.
- Stop declaring volatile from transport primitives (Section 6).
- *Deletes:* the `:*_declared` flags, `_INTERNAL_FLAGS` lists, IMPURE.
- *Tests:*
  - edit an unrelated cell: the registrar still primes;
  - a run that declares nothing clears the record;
  - a sweep cell has no volatile;
  - a `connected` cell's reader is not restored stale.

**Step 5. Value identity and one replay helper (R7).**
- Presync, prime and resource keyed on the memo key's value identity.
- Prime and resource failures surface as the reading cell's error.
- Writer resolution taken from `cell.deps`.
- *Deletes:* the `using` mirror and three of the four first-writer scans.
- *Tests:*
  - slider-driven theme reaches the region;
  - reactive value reaches a region reader;
  - a failed resource open retries.

**Step 6. WorkerRecord (R8).**
- Lifecycle state, liveness oracle, `kill_worker!`, `forget!`, in-flight reconciliation after attach.
- Liveness pings run concurrently and never take `k.lock` from the supervisor.
- Fold telemetry thresholds and ENV tunables into `_rcfg`.
- *Deletes:* `_REAPED` TTL, `_KERNEL_UNRESPONSIVE_SINCE/_BUSY_SINCE`, the SIGTERM-only teardown kill, about five hand-copied eviction blocks.
- *Tests:*
  - a busy worker survives reattach;
  - a deadlocked worker is killed on Restart;
  - a reused port after a reap is adopted.

**Step 7. Region lease (R9).**
- One state machine, tri-state scheduler answers, release by job id, confirm before forgetting, a single placement-drop function, a per-(host, name) submit lock, explicit close-grace policy.
- *Deletes:* `_GAVE_UP`, `_GRANT_GRACE_S`, `_WITHDRAWN_AT`/`_RUN_ASKED_AT` comparisons, the duplicate drop blocks.

**Step 8. Sweep ownership (R10).**
- The hub supervisor is the only advancer.
- Polls are tri-state.
- The supervisor emits settle to the owning notebook.
- One reset/cancel implementation, holding the store lock.
- *Deletes:* the card-poll `submit=true` and the `settled` action.

**Step 9. Transport typed results.**
- `_channel_dead!` closes its session.
- `_reach` goes through `_open_tcpip`.
- Wait loops exit on `!alive`.
- Deadlines start at request time.
- ProxyJump: implement it or fail loudly when it is set.
- *Deletes:* message-text matching and the stale `ssh -L` readiness wait.

**Step 10. Browser merge.**
- Per-cell rev-gated merge in `_publishState`.
- A sequence number on cell patches with gap resync.
- Worker dot driven from facts.
- *Deletes:* the 250 ms / 2 s timing pulls.

**Step 11. Provisioning identity.**
- One EnvIdentity value, with "built" and "compiled" as separate records.
- The payload SHA leaves the env fingerprint; repointing the worker code is a cheap develop step.
- One shell builder for Julia per machine.

---

## 6. Why does an `@sweep` job cell show "volatile"?

**Where it comes from.** `Sweep.connected(host)` and `Sweep.run_there(...)` both call `_declare_volatile()` on every call (remotestore.jl:138-152 and 259-260, checked). That pushes a `:volatile` record into the effect sink of whichever cell is evaluating at the time.
- A cluster `@sweep` reaches these helpers inside the job cell's own run: reachability check, metadata sync, scheduler polls, submission.
- Each call adds another identical record, with no deduplication, so the tooltip repeats the `@sweep` statement many times.

The hub then does three things (server.jl:1700-1741, checked):
1. Adds `:volatile_declared` to the cell's flags.
2. Persists the records to the EffectStore under the cell's source hash.
3. Ships `output.effects` to the page (server_history.jl:1114-1121).

The page's `_effectBadge` (view.js:650-662) hides only `value_identity`, so `⚙ volatile` is what remains. `cell_json` also sends the flag as a tag (server_history.jl:1110), so the tag editor shows `volatile_declared` as a removable "Custom" chip. Job cells always re-run on open, so the chip comes back after every load. Whether it appears at all depends on whether that run happened to reach one of those helpers. It is not a stable property of the cell.

**What it was built for.** Commit 26632d1e added it for CODE cells that ask a host something, such as `ok = Sweep.connected(host)`. Without it, a cached "signed in" answer would be restored next session and contradict the padlock. On a CODE cell it makes that cell uncacheable (deps.jl:965, eval.jl:1078) and makes region boundaries transfer its value instead of re-running it.

**What it implies for a job cell: nothing.**
- `_cell_effect` returns PURE for every non-CODE cell before it looks at any flag (deps.jl:960).
- `_memoizable` rejects non-CODE cells anyway (eval.jl:1049), and `_memo_status` is CODE-only.
- `_reestablish_effects!` skips non-CODE cells (server.jl:1752), so the EffectStore record for a job cell is written and never read.
- Job cells always run on main (`_cell_region` returns ""), so the region transfer policy does not apply.
- `_key_poisoned` does not read the flag (eval.jl:1238). Readers of the sweep are keyed through `:value_identity` (sweep.jl:5370-5375, server.jl:1714-1723), which moves their memo keys when results land.

**Should it show? No.**
- It is noise for the reader. `value_identity` was silenced in the badge for exactly this reason (view.js:643-648, commit cc99ab38), and `volatile` on a job cell is the same kind of internal bookkeeping.
- It is also semantically wrong. `:value_identity` says "this value is not a function of my source, and here is its tracked identity". `volatile` says the opposite: "untracked, a cached value would be a lie". The two coexist only because every consumer happens to ignore `volatile` for JOB cells.
- That makes it a trap. The obvious fix for the real CODE-cell bug (adding `:volatile_declared` to `_key_poisoned`'s flag test) would poison everything downstream of every cluster sweep, the outcome commit 3fd13cb4 was written to avoid.

**Recommendation:**
1. Declare volatility where a live answer is handed to user code, not inside transport primitives. Either `run_sweep` suppresses the effect sink for its internal reconcile and sync traffic, or `_declare_volatile` is called from the public `connected`/`cluster_status` entry points and not from `run_there`. A record of `value_identity` in the same harvest should supersede `volatile`.
2. Route `_key_poisoned` through `_cell_effect` (Step 0). CODE readers of a host-querying cell then stop restoring stale results, and sweep readers stay keyable because JOB cells classify as PURE.
3. Keep internal classifications out of `tags`.
4. As a stopgap, add `volatile` to `_EFFECT_SILENT` for non-CODE cells.

On CODE cells the declaration is real but only half-wired. Its readers are not poisoned, and any edit to any cell drops the flag (Section 2.D). Step 4 fixes both.
---

## 7. Second verification pass

The first pass sent only about half the findings to a verifier. A second pass covered the other 65. All 114 findings now have a verdict: 97 confirmed, 17 plausible, none refuted. Per-finding verdicts and reasoning are in `_audit-cluster-runtime-2026-10.findings.json`.

These corrections change what the sections above say:

- **Section 6, record count.** The claim that the volatile tooltip repeats the `@sweep` statement many times is wrong. `_harvest_effects` (capture.jl:757-769) deduplicates by kind, names and statement, so a job cell carries one record. The declaration also lands only on cluster sweeps: a local sweep does not reach `connected`.
- **Defect #21 (sweep settle).** This is narrower than stated. A reloaded page sends `settled` on its first poll, because its card was rendered unsettled. The gap is a sweep that settles while no browser is open on the notebook.
- **Defect #20 (browser RUNNING).** Lowered to medium. It is display state only, and the hub's state is correct.
- **Defect #19 (payload SHA in env fingerprint).** Confirmed, but the payload path has to reach the env, because the Manifest records the per-version develop path of SlateWorker and the SDK. The fix is to re-point the two `Pkg.develop` paths without a rebuild or a prepare gate. The SHA should not simply be dropped.
- **Section 2.D (tag editor).** The browser tag popup preserves internal flags. They are lost when a chip's × is clicked, when an agent passes an explicit tags list, and on any source edit (`update_source!`).
- **Section 2.B.** `_restale_side!` skips the locked guard on purpose: its comment says locked cells must go stale, because the new process does not hold their result. It does unblock and bump rev.
- **Restart kill (SIGTERM only).** Lowered to medium. It needs a worker that ignores SIGTERM.
- **`_key_poisoned` fix.** Routing it through `_cell_effect(c) == VOLATILE` is safe for sweep readers, because JOB classifies as PURE. Only a raw flag test would poison them.
- **Dead-wire grace const.** Lower impact than claimed. Region workers override it with a measured grace, and `KAIMONSLATE_EVAL_TIMEOUT` is read at runtime.

Confirmed in the second pass, previously reviewer-traced only, among the high defects:
- #14: the card poll submits from a cached scheduler answer.
- #15: a failed scheduler poll reads as "nothing live". `cancel!` cancels nothing and still writes `.cancelled`.
