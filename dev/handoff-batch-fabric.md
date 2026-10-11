# Handoff — feat/batch-fabric

Worktree `~/devel/KaimonSlate.jl/.claude/worktrees/feat+batch-fabric`, served by the **`slatebf`**
Kaimon extension on `http://127.0.0.1:8901`. Suite green at **8749**.

## Landed (9 commits)

| | |
|---|---|
| `5c65e358` | **The hub owns the ssh sessions.** A worker delegates over the gate (`__slate_sshop` → `__slate_sshop_reply`), so a 2FA cluster costs one sign-in per machine instead of one per process. Also: refused logins no longer leak sockets, a login in flight is not replaced, archiving is stdlib `Tar`. |
| `d934f686` | **A region's node is reached through its login session.** Routed workers bind `0.0.0.0` + CURVE; the data channel takes the same route; `region=` beats an inferred `import_scaffold` so a pinned cell's data crosses the boundary; a queued region resolves itself. |
| `fea111c9` | **Sign-in panel** (`assets/js/sessions.js`) — keyed by host, non-authenticating probe, ⌘K + padlock. |
| `bd9d4429` | An armed sweep **waits** for a sign-in rather than failing to provision. |
| `9dcde66c`, `640abb29`, `0baecd81` | Shared stylesheet `assets/shared.css` + `test/js/shared_styles.mjs` guard. |
| `59f11719` | Messages point at the padlock; signing in re-arms whatever was waiting. |

## Landed since (5 commits) — a live run against the 2FA cluster

Every one of these was found by driving `auth_demo` end to end, not by reading code.

| | |
|---|---|
| `80c9989a` | **`/api/scheduler` was replacing the hub.** It used `h` as a scratch local; every handler in `_make_router` closes over the same `h::Hub`, and Julia boxes a captured variable assigned anywhere in the scope — so opening the region form on a host you had not signed in to yet turned the hub into `nothing` and killed every route that touches it. The guard walks `_make_router`'s AST for any assignment to an enclosing-scope name. |
| `26632d1e` | **A cell that asks a host something is no longer cached.** `Sweep.connected`/`run_there` declare `slate_effect(:volatile)`; `:volatile_declared` is restored from the durable EffectStore, so it holds from t=0 and a run-all asks again instead of restoring the last session's answer. |
| `cd276d29` | **A placement expires with its allocation** — lease from `squeue %L`, so an ADOPTED allocation is trusted only for the time it has left. Plus: a routed node's unreachable message stops recommending key auth. |
| `df7d86de` | **A dropped session takes its forwards and its wires with it.** `SshTransport.on_drop!` announces; the data-tunnel cache and the kernels riding that session are discarded at once. `:direct` workers exempt. |
| `67e4afa0` | The worker note chip wraps instead of clipping (cascade tie against `.wchip`), and is no longer printed twice. |

## Landed since (7 commits) — scheduler options, said on the cell

| | |
|---|---|
| `5fc772ba` | **Every scheduler setting reaches the job.** `gpus=`/`nodes=`/per-cell `account`/`qos` were accepted, parsed, and never emitted. Now pass-through: anything that is not one of Slate's own cell settings becomes `--key=value`, `_` → `-`. `_ATTR_RESOURCES` is the CATALOGUE (types counts, feeds the editor), not a permitted set. Nothing had ever asserted the script's contents. |
| `ce8c4b15` | **Per-cell options live in the `Slate.sweep` footer**, as JSON — one object per cell, per line, sorted keys. Header values lose anything outside `[A-Za-z0-9_.:+/@-]` to the tag sanitiser, which rules out `--licenses=ansys@srv`, a constraint expression, or a space. Merged OVER the header attrs in `_attr_args`, so old notebooks keep working. |
| `3aefc2d2` | **The pair-list editor** — autocomplete over `/api/sched-options` (served from Julia so it cannot drift), warn-not-block on unknowns, red on a non-numeric count, cluster value as placeholder. Own menu, not a `<datalist>` (which dumps all 18 on focus and cannot be capped from CSS). |
| `33ba935e` | **`script = "model.jl"`** — read via the same resolver `@asset` uses; its source rides `setup_src`, which both ships to the compute node and is digested into the sweep key. Editing the file moves the key; a bare `include` would do neither. |
| `bc72598f` | **A sweep submitted from its card announces it finished.** `watching` was seeded false when the card rendered `:ready`, and only set when a poll timer starts — which never happens if the work finishes before the submit's reply lands. Everything downstream then sat stale. |
| `b0438142` | **One metadata sync at a time per mirror.** `pull_meta!` wipes and re-extracts; two concurrent syncs (a cell + its own card's poll) gave `rm: ENOTEMPTY`. |
| `9f8e1f0c` | **A component kwarg decorates what the chart kind generated.** `echart(:heatmap, xs, ys, z; yAxis=(name="b",))` replaced the category axis, dropping `type`/`data`. |

Merged `main` after these (10 commits: vim mode, app region cells, worker tool, 1.4.7). Four conflicts —
see "Traps" for the one that matters.

## PBS — DONE (uncommitted at time of writing)

Implemented, unit-tested, and driven against a real OpenPBS cluster built for the purpose.

**Julia side.** `SlurmTarget` is now `ClusterTarget` with a `kind` field (`SlurmTarget`/`PbsTarget`
are constructors that set it); which scheduler a cluster runs is a property of the cluster, and
`launcher_for` is the ONE place it is consulted. `allocation.jl` keeps one flow and gives each
scheduler three scripts — find, request, release. `_VIA` carries the kind so `_run_on` knows how to
get into an allocation. Every scheduler command now lives in exactly four places: the two launchers,
those three script pairs, `_in_allocation`, and detection.

**PBS is not SLURM with different flag names**, and three things are decisions rather than
translations:

- Per-node resources go INSIDE a chunk statement (`-l select=N:ncpus=…:mem=…:ngpus=…:mpiprocs=…`);
  job-wide ones sit beside it. `select=` written out wins outright — one escape hatch for every
  site chunk resource Slate has no name for, which is what the unmappable settings point at.
- A setting one scheduler cannot express **errors, naming the way to say it there**
  (`constraint`/`gres`/`nodelist`/`exclude`/`reservation`/`mem_per_cpu`/`ntasks` on PBS; `select` on
  SLURM). Never silently dropped — that was the failure `5fc772ba` existed to end.
- Unknown keys still pass through, but as `-l key=value` **as written**: PBS resource names carry
  underscores, so sbatch's `_` → `-` is wrong here.

Plus: no `--dependency=singleton` (a `qselect -N` guard in `submit!` does the same job), no
`--no-shell` (an allocation is a job that sleeps out its walltime), and no `srun --overlap` (a
compute node is reached by ssh from the login node, which is what every PBS site already does).

The cell editor's option catalogue now carries BOTH spellings in one list and greys out what the
cell's cluster cannot say. `docs/src/clusters.md` has a "SLURM and PBS" section.

### The PBS test cluster — `~/devel/SlateCluster.jl/cluster-pbs`

`./up.sh`, host `slate-pbs` on port 2224. Runs alongside the SLURM one, deliberately: the
interesting bugs in a two-scheduler fabric are the ones where one scheduler's spelling reaches the
other, and you cannot find those with one cluster up.

OpenPBS 23.06.06 is **built from source** — there is no packaged PBS anywhere, and the only binaries
OpenPBS ships are amd64 debs for Ubuntu 18.04. ~5 minutes, once. That directory's README lists the
five things that will otherwise waste an afternoon.

### Testing — `cluster/smoke.jl`, and no more shell

`smoke.sh` and `integration.sh` are GONE, in both cluster directories. One Julia file now serves
both clusters:

```
julia --project=$KAIMONSLATE cluster/smoke.jl slate-pbs   pbs      # 32 pass
julia --project=$KAIMONSLATE cluster/smoke.jl slate-slurm slurm    # 30 pass (2 are PBS-only)
```

It drives the SHIPPED functions — `SchedulerDetect.detect`, `find_allocation`, `route!`/`_run_on`,
the launchers, `provision_payload!` — over the real ssh transport, instead of re-stating in `bash`
what they are supposed to do. That is not a style preference. **A shell restatement can pass while
the code it paraphrases is broken**, which is exactly what happened: `smoke.sh` was green on a
hand-written `qstat` pipeline while `poll` was parsing nothing.

Its last testset ships `integration.jl` to the login node and runs it there — a real 24-shard sweep
across both compute nodes. `SLATE_SWEEP=0` skips it.

### Both halves proven on PBS, live

The BATCH half is covered by `smoke.jl` (a 24-shard sweep across both nodes). The INTERACTIVE half
was proven separately, through a notebook region — `notebooks/pbs_demo.jl`, cell `on_node`, tagged
`region=pbsnode`:

```
provision DONE host=c1
spawn: launching worker on c1 (port=9104) region=pbsnode
connect OK: attached to worker on c1:9104 → notebook now runs on c1
data channel: opened dedicated tunnel c1:9106 ← 127.0.0.1:61313 via slate-pbs (CURVE)
→ (ran_on = "c1", cpus = 4, julia = "1.12.7", store = "/scratch/slate-pbs is here too")
```

Allocation → `route!` → provisioning over the ssh hop → worker on the compute node → CURVE plus a
data forward on the login node's session. `/api/allocation/release` gives it back (the job is gone a
few seconds later — a cancelled PBS job sits in `E` first, which is the same behaviour `cancel!`
had to be rewritten around).

One piece of noise worth cleaning up, seen in that log: the CURVE key probe
(`head -n1 ~/.cache/kaimon/curve/server.key`) logs `ssh c1 FAILED` four times before the worker has
written the key, then succeeds. A precondition being retried, logged as a failure — the same shape
as the `_send_dir!` retry noise already listed under "After that".

### Four bugs the cluster found that unit tests could not

1. **`qstat -f -u "$USER"` silently overrides `-f`** and prints the short table, which has no
   `Job_Name` column. A poll written that way parses as "no jobs anywhere" — a running sweep reads
   as one that never started. `poll` and `find_allocation` resolve ids with `qselect` first. The
   unit-test fakes now reproduce the override and `smoke.jl` asserts it, so it cannot come back.
2. **`-o <dir>/` names the output after the JOB ID**, which is the one thing the fabric never keeps.
   `logs` is asked for a NAME, so it found nothing, ever. The script now names the file, with
   `^array_index^` for an element.
3. **`cancel!` always returned 0.** A cancelled PBS job goes to `E` (exiting) before it leaves, so
   re-polling straight after a successful `qdel` still reports it live. PBS hands us ids, so the
   count comes from `qdel` itself — SLURM only re-polls because `scancel` exits 0 whether or not a
   name matched.
4. **`integration.jl` submitted once and then only watched**, so the probe wave's one chunk ran and
   the other three were never sent. It reconciles each pass now, which is what a sweep cell does.

## A released allocation leaves its worker running — FIXED

Measured on the PBS cluster: after `/api/allocation/release`, a worker was still alive on the node
14 minutes and several `qdel`s later, and the notebook kept happily running cells on it. Its pid was
unchanged across three releases.

The cause is structural and applies to PBS especially: the worker is started by an **ssh from the
login node**, so it is not in the job's process tree and `qdel` does not kill it. A real site's
epilogue or cgroup would usually reap it, but relying on that is wrong — where it does not, the
worker is squatting on a node the scheduler has already given to someone else.

`region_release!` now reaps the region's workers before releasing, so the route and the idle path
both get this right. It also drops the node's route and stops its file syncer: a watcher left
pointed at a released node pushes the next local edit to a machine the scheduler has reassigned.

## Only two or three notebook tabs can be open at once

A fourth tab hangs on a spinner until one is closed. Measured 2026-09-05: Chrome held 7 established
connections to the hub with a few tabs open, and a notebook tab costs about three — the per-page
WebSocket (`wscall.js`), the `/api/events` SSE stream (`panels.js:448`), and an idle keep-alive
connection left by ordinary fetches. HTTP/1.1 allows about six per origin, so the limit arrives at
two or three tabs. Transient streams (publish, preflight, import) come out of the same budget, so
starting a publish with two tabs open can tip it over mid-operation.

Workaround: `localhost:8901` and `127.0.0.1:8901` are separate origins with separate pools.

HTTP/2 would multiplex these, and HTTP.jl 2.6.6 does implement it (client and server, ALPN over TLS
plus cleartext with prior knowledge). It does not help here. Browsers negotiate HTTP/2 only over
TLS, so it would need a trusted certificate on loopback, and HTTP.jl refuses WebSockets over HTTP/2
outright (`http_websockets.jl`: "WebSocket upgrade over HTTP/2 is not supported"), so the socket
would still take its own connection.

Three steps, each useful alone:

1. Move `/api/events` onto the existing WebSocket. One less connection per tab, no new concepts.
2. Make the WebSocket endpoint hub-level (`/api/ws`, notebook id per message) instead of
   `/api/<nbid>/ws`. Today two tabs on different notebooks cannot share a socket because the URLs
   differ.
3. Hold that socket in a `SharedWorker`, one per origin. Persistent connections then stop scaling
   with tab count.

## `warm` is off for a scheduler region

A warm worker skips the boot by living on the host between notebooks. A scheduler region has no such
host: the node is an allocation, the next one may be a different machine, and workers on the old one
are gone with it. `warm > 0` is also what stops `_region_reconcile_impl!` releasing an idle node, so
a scheduler region with warm workers holds and bills for one indefinitely, re-requesting as each
walltime expires.

Coerced to 0 in `_region_from_dict`, so a record already on disk with a count stops holding as soon
as it is read; the region form shows why instead of the field.

## Next — releasing a node when the work is done, not when the clock runs out

An allocation bills for the time it is HELD. Today it is given back in three places, and all three
are explicit: the region is deleted (`region_reap!`), it is drained to zero warm workers with none
live (`_region_reconcile_impl!`), or someone presses the button (`/api/allocation/release`).
Ending the job is the whole mechanism and it returns everything the scheduler granted — cpus,
memory and GPUs alike; neither scheduler has a separate GPU release, and neither needs one.

What is missing is the case in between: a notebook that finishes its GPU work at minute 2 of a
60-minute walltime keeps the node for the other 58. Nothing is wrong — it is just paid for.

`dev/Cluster-user-feedback.md` asks for exactly this, unprompted: *"I'd ideally (manually or
automatically) free the GPU allocation when all cells that require GPU access have completed"*, and
notes the workaround people actually use — asking for a short walltime in case they forget.

The pieces are already there: `_region_has_live_workers`, `_region_holds_node`, and the reconcile
tick that already releases on drain. What it needs is a policy — an idle timer on a region
(`release after N minutes with no cell tagged for it running`), which is a field on the region form
and a check in `_region_reconcile_impl!`. Worth agreeing the default before building it: too eager
and an iterative debugging session pays a queue wait between every run, which is the failure mode
that makes people stop using it.

## After that

**A `BLOCKED` cell state.** A cell that can't run for want of a sign-in is currently
`mark_errored!` — it reads as a failure, and its downstream cells then throw spurious
`UndefVarError`s for values that were never computed. It should be a distinct state shown as a
header chip (reason + action), not a red body. Touches `deps.jl` (state enum, `mark_blocked!`),
the runner (don't retry, but clearable), `cell_json`, and the front end. The re-arm hooks it needs
already exist: `_place_in_background!` (node granted) and the `SshAuth.set_reporter!` in
`_install_sshauth_watch!` (signed in), both of which call `_restale_region_cells!`.

Agree the design before building — a new cell state has implications for the DAG, the memo layer
and the export path.

Still the right next piece, and now the ONLY thing left of this shape: every message a blocked cell
shows is honest (`df7d86de`, `cd276d29`), but the cell is still red and its downstream still throws
a spurious `UndefVarError`. A second symptom belongs with it: while a sign-in dialog is open,
provisioning retries `_send_dir!` against the unauthenticated host every ~2s and logs a bare
`FAILED: sending …` each time — a precondition being retried as though it were a failure.

## Before merging

- **`kaimon.toml` is deliberately uncommitted** — it registers this worktree as a second extension
  under the `slatebf` namespace. Revert it.
- Untracked and undecided: `dev/Cluster-user-feedback.md`, `docs/_batch-fabric.md`.
- The cluster work is in a DIFFERENT repo (`~/devel/SlateCluster.jl`) and is also uncommitted:
  `cluster-pbs/` (new), `cluster/smoke.jl` (new), `cluster/integration.jl` (reconciles each pass),
  and the two deleted `.sh` files.
- `docs/src/clusters.md` is current; the peer-mesh path (`peer_mesh.jl`, `worker.jl`) still shells
  out to `ssh -N -L` and was out of scope.

## Testing against the 2FA cluster

`~/devel/SlateCluster.jl/cluster` — `./up.sh --2fa`. Password `slate`; a valid code from
`./auth.sh --code`. The `auth_demo` notebook is the end-to-end exercise (sweep → region on a
compute node → values combined locally).

Signing in without a browser, which is how most of this was verified:

```julia
# on the KaimonSlate.jl gate session, not a worker
pp("/api/host-session", Dict("host" => "slate-2fa"))          # async; answer while it is open
get_("/api/sshauth")                                          # pending prompts
pp("/api/sshauth", Dict("id" => p["id"], "answer" => …))      # password, then the OTP
```

Restarting the extension drops every session — that is correct now that the hub holds them.

## Traps worth not rediscovering

- **A worker loads in the NOTEBOOK's project**, so payload files may only import stdlibs. An
  `import CodecZlib` in `remotestore.jl` took every worker down at boot. `test_regions.jl` guards it.
- **The home page does not load `notebook.css`** — it has its own inline sheet and defines none of
  the theme variables. Anything shared goes in `shared.css` with a fallback on every `var()`.
  `test/js/shared_styles.mjs` guards it.
- **`sessions.js` is an ES module**, so it cannot see `nbState` (a classic-script binding). It reads
  the notebook id from the `/n/<id>` URL.
- **The `auth_demo` env has no pinned manifest.** Adding any package re-resolves freely; adding
  `QRCoders` dragged Makie back to 0.21.9, which breaks on Julia 1.12 (`TypeName.mt`). Repaired with
  `Pkg.update()` to Makie 0.24.13. Test a resolve on a copy of the env before adding anything.
- **This branch REPLACED the ssh-subprocess transport with libssh2 sessions**, so anything about
  `ControlMaster`/`ControlPath`/`_ssh_mux_*` arriving from `main` is dead code here — the `main`
  merge brought a testset for it that had to be deleted, not satisfied. Expect the same on the next
  merge if `main` touches that area.
- **A captured variable assigned anywhere in `_make_router` is shared by EVERY handler** (Julia boxes
  it). Using one as a scratch local replaces it for all of them. Guarded by `test_complete.jl`.
- **A `:tunnel` worker's wire is a forward on its host's session and cannot outlive it** — and a dead
  forward is indistinguishable from a live one until something waits out its receive timeout. Never
  poll for this; the transport announces it (`SshTransport.on_drop!`).
- **`_DATA_TUNNELS` is keyed by the NODE, but the forward rides the LOGIN node's session.** Any
  eviction by host has to follow `_VIA` or it misses exactly the compute-node case.
- `@asset` does **not** work in a `@web` cell — sections must be string literals, and `{{ }}` in a
  JS pane escapes to a JSON literal. Use a code cell returning `WebPage(js = @asset(…) * …)`.

## Working preferences observed

Don't sleep waiting on background work, and don't re-run the full suite repeatedly — start it and
carry on. Use the Edit tool, not shell heredocs. Comments state the rule, never the debugging
story. Short commit messages: why, not what.
