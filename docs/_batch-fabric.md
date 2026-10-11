# Batch fabric: fan-out onto SLURM and similar schedulers

Working design doc. Branch `feat/batch-fabric`. Status: BUILT — this is the design as first
written, kept for the reasoning; the code is the current truth.

Two things below were overtaken by what got built:

  * **"write to shared CAS"** assumed the hub and the compute nodes see one filesystem. They do
    not: the deployment is Slate on a laptop driving a cluster it has no mount on. Metadata is
    mirrored into a local shadow with rsync; result data stays put and is read by byte range over
    a multiplexed ssh connection (`src/remotestore.jl`, `SshSource` in `src/sweep.jl`).
  * **Storage form.** A unit's result can be stored ADDRESSABLY (`data=auto`) rather than whole,
    so a slice costs the bytes it names — `src/dataset.jl`.

The worked examples are `SlateCluster.jl/notebooks/`: `batch_sweep_demo` for the lifecycle,
`dataset_demo` for the data half.

This describes a second execution model for KaimonSlate, alongside regions: submitting
parameter sweeps and other embarrassingly-parallel work to a batch scheduler (SLURM first,
PBS/Torque anticipated, Kubernetes named as a target that must not require a rewrite).

## 1. Why this is not regions

Regions are a real-time model. A region is a live worker on the far end of a ZMQ wire, with
ports, CURVE keys, an `ssh -L` tunnel, a blob channel, and telemetry. The hub owns that worker.
If the hub dies, the session dies.

Batch inverts every one of those properties:

| | Region (mesh) | Batch fabric |
|---|---|---|
| binding | live ZMQ wire, RTT-bound | none; CAS plus files |
| latency | 1 to 40 ms | seconds to hours (queue) |
| state | namespace in worker RAM | no namespace; stateless task |
| failure | disconnect, then reattach | job dies, re-run shard |
| data | push/pull over blob channel | write to shared CAS |
| lifecycle | hub owns the worker | scheduler owns the job |
| cell semantics | synchronous eval | asynchronous; a future |

The last row is the architectural divergence. A batch cell submits work and returns while the
work continues. The notebook must be closable, the hub restartable, and the user able to return
a day later to a finished result.

That forces a different role for the hub. For regions the hub is an **orchestrator**: it holds
the wire and owns the lifecycle. For batch the hub is a **reconciler**: all authoritative state
lives on durable storage, and the hub's job is to observe what exists and submit what is
missing. It can be killed at any point and resume with no loss.

Regions and batch share exactly two things: the content-addressed memo store (`src/memostore.jl`)
with its portable digests, and provisioning. Dispatch, lifecycle, data movement, failure
handling, and UI are all separate. Nothing else should be shared.

## 2. What this is for

By job count, academic clusters are dominated by array jobs: parameter sweeps, per-sample
pipelines, Monte Carlo, hyperparameter search. That is the workload to own.

Running a sweep is not the pain. `sbatch --array=1-10000` works and everyone knows how. The
pain is bookkeeping:

- 47 of 10,000 jobs failed and the user does not know which, or why.
- Which parameter combinations have actually been run? Is this output directory current with
  the code that produced it?
- The analysis changed. Does the cluster half need to re-run?
- 200 more sweep points are needed without redoing the first 4,000.
- Getting 10,000 output files back into a plot.
- A reviewer asks what produced Figure 3.

Those are provenance and resumption problems. Slate's content-addressed store, dependency DAG,
and publish pipeline already address them for local work. Extending them across a scheduler is
the actual contribution.

### Out of scope

- **Large MPI simulations.** Slate should submit, monitor, and analyze them. It should not try
  to be the compute.
- **Containerized multi-stage genomics pipelines.** Nextflow and Snakemake are better at this
  and we will not beat them.

### Incumbents

- **DrWatson.jl**: `dict_list`, `produce_or_load`, `@tagsave`. The Julia standard for this.
  Filename-keyed rather than content-keyed, and not interactive.
- **Snakemake / Nextflow**: own multi-stage file pipelines.
- **Dask / Ray**: Python.
- **ClusterManagers.jl**: `addprocs_slurm` holds an allocation open and has workers dial back to
  a master. That is the mesh model, with the failure mode this design exists to avoid.

## 3. Decisions taken

| decision | choice |
|---|---|
| shard result | Julia return value plus optional file artifacts |
| flagship use case | parameter sweep with a live-filling plot |
| partial results | live-updating table; downstream cells re-run as shards land |
| schedulers | SLURM now; PBS/Torque anticipated; Kubernetes must not force a rewrite |
| test bed | local SLURM cluster in Docker |

## 4. Architecture

### 4.1 Three interfaces

Kubernetes breaks any single abstraction, because it has no shared filesystem, no `$HOME`, and
uses images instead of provisioned environments. So the abstraction splits three ways:

**Launcher**: how work gets scheduled.

```julia
submit(launcher, spec::JobSpec)     -> Handle
poll(launcher, handles::Vector)     -> Dict{Handle,State}   # BATCHED
cancel(launcher, handles::Vector)
logs(launcher, handle)              -> String
```

`poll` must be batched. One `squeue -j <comma-list>` for 500 array elements, never 500 ssh
calls. Backends: `ExecLauncher` (today's ssh plus nohup), `SlurmLauncher`, `PBSLauncher`,
`K8sLauncher` later.

**Provisioner**: how code and environment reach the node. Shared-FS backends rsync a project
tree and populate a depot. Kubernetes bakes an image. These are genuinely opposed and should
not share an interface.

**Store**: CAS transport. Shared filesystem, object store, or ssh pull. The CAS layout itself
(`blobs/sha256/<hh>/<h>`) does not change.

### 4.2 The reconciler

State is derived, not stored. Recording "cell X is running as job 12345" drifts the moment the
hub stops watching: the job finishes while the hub is down, an admin cancels it, or the hub
crashes mid-write.

Instead, every shard has a content-derived identity, and the world is read:

| observation | state | action |
|---|---|---|
| output blob exists in CAS at `fullkey` | done | never submit |
| scheduler reports a live job named `fullkey` | running | never submit |
| no live job, no output, prior attempt | failed or lost | surface; resubmit on request |
| neither | missing | submit |

Reopening a notebook is: recompute shard keys, one CAS listing, one batched `squeue`, render.
Nothing is remembered. Everything is observed. This survives a hub restart, a machine reboot, a
Slate upgrade, and opening the notebook from a different laptop. Because reconciliation is
idempotent, crashing halfway through submission is harmless.

Reattachment needs no network wire at all. Just `squeue`.

**Site caveat**: `squeue` only lists live jobs; completed ones age out and need `sacct`, whose
availability depends on the site having accounting configured. "Done" must therefore be answered
by the CAS. `sacct` is only consulted to explain a failure.

### 4.3 Shard identity

```
fullkey = digest(closure source, captured values, param, env manifest, CacheFlags)
```

The closure source and param are recomputable from the notebook. Captured values are runtime
data and are not. So the sweep manifest (the shard key set plus capture digests) must be
recorded.

It is recorded in the **fan-out cell's own memo entry**. Not a parallel job database, and not
the notebook footer. `docs/src/memoization.md` already promises that reopening a notebook
restores results instead of recomputing, so reopen becomes an ordinary memo restore that hands
back the manifest, after which reconciliation takes over. No new persistence layer.

Two consequences fall out:

- Upstream changes give the cell a different memo key, the memo misses, and the user correctly
  gets a new sweep rather than silently reusing stale shards.
- The durable footer stays small. Putting 4,000 shard keys in it would compound the git-noise
  problem that footer already has.

### 4.4 Preventing double submission

Two guards, of different strength.

The weak guard is DAG invalidation deciding the fan-out cell is fresh. Useful, with the same
blind spots as staleness analysis everywhere else.

The strong guard is structural: **the submit plan is computed after CAS lookup**. If the cell
re-runs, every shard resolves to an existing blob and the plan comes out empty. Correctness here
does not depend on invalidation being right; it depends on content addressing, which is already
proven portable across hosts.

For duplicate submission across a hub restart, name the scheduler job by the chunk key. Then
`squeue --name=<key>` is the claim check and `--dependency=singleton` makes the cluster itself
refuse duplicates. This avoids `O_EXCL` claim files on NFS, which is an unreliable primitive.

On top of both, a hard cap and explicit confirmation on submit count, in the spirit of the
existing transfer cost gate. Nothing should quietly fire 10,000 jobs.

## 5. Provisioning and precompilation

Today nothing in `src/` sets `JULIA_DEPOT_PATH` or `JULIA_CPU_TARGET`, so every provision bakes
native code for whichever machine ran it (`cpu_target=native` is the Julia default, confirmed on
1.12.7).

"Different architecture" is three problems:

**Same ISA, different microarchitecture** (login node Skylake, compute nodes Zen3 or Sapphire
Rapids, or mixed partitions). This is the common case and needs no target-node build. Set
`JULIA_CPU_TARGET` to a multiversioned string, the technique the official Julia binaries use:

```
JULIA_CPU_TARGET="generic;sandybridge,-xsaveopt,clone_all;haswell,-rdrnd,base(1);skylake-avx512,-rdrnd,base(1)"
```

Multiple variants are baked into one pkgimage and dispatched at load. Costs image size and
precompile time. Buys immunity from the whole class of problem.

**Different ISA** (aarch64, POWER), or anything hardware-conditional such as CUDA.jl selecting a
toolkit or MPI.jl binding a system MPI through Preferences. Multiversioning cannot cross ISAs
and Preferences resolution needs the real hardware. Here the build must happen on the target.

**Heterogeneous partitions**: key the depot by target class,
`depot_root = /scratch/$USER/slate/depot/<arch>-<partition>`. N depots, each provisioned once.

### 5.1 The provision job

Provisioning on the target runs as a **batch job**, not a held-open interactive session:

- It queues normally; nobody babysits an `salloc`.
- `--dependency=afterok:<provision_jobid>` lets the provision job and the fan-out array be
  submitted together, with the scheduler enforcing ordering. This survives the hub going away.
- `--dependency=singleton`, named by the manifest digest, gives free mutual exclusion. Two
  notebooks provisioning the same environment cannot race.

Cache key is `_manifest_digest` (already portable) crossed with depot class. Same manifest, same
class, the job is a no-op. This is the same idempotency `provision_remote!` already has,
relocated onto a compute node.

The UI half largely exists: `src/prepare.jl` parses streaming `Pkg.instantiate()` and
`Pkg.precompile()` output into structured status, including for remote provisions
(`server_complete.jl:2717`). The provision job writes output to shared scratch and the hub tails
it through the same parser.

### 5.2 Two traps

**Thundering herd.** If 200 array tasks start against an incomplete depot, each tries to
precompile into it concurrently, which can take down a shared filesystem. Set
`JULIA_PKG_PRECOMPILE_AUTO=0` in every task job so a task structurally cannot precompile: it
loads from a ready depot or fails fast.

**CacheFlags must match.** The precompile cache path incorporates `check_bounds`, `opt_level`,
`inline`, `debug_level`, and `pkgimages`. The codebase already exposes
`effective_worker_extra_flags`, so adding `--check-bounds=no` to worker flags would silently
invalidate the shared depot and send every task down the recompile path. The provision job must
run under exactly the task jobs' flags, and this should be asserted rather than assumed.

## 6. Progress and data flow

### 6.1 Shared filesystem realities

SLURM and PBS clusters essentially always have a shared filesystem, in three tiers: `$HOME`
(NFS, quota'd, slow for many small files), parallel scratch (Lustre/GPFS/BeeGFS, fast for large
sequential I/O, often purged), and node-local scratch (fast, wiped at job end). The CAS belongs
on parallel scratch.

Two properties constrain the design:

**Close-to-open consistency.** NFS guarantees a writer's `close()` is visible to a later
`open()`. It does not guarantee timely visibility of appends to an open file. Tailing a progress
log from the login node is exactly the pattern that silently fails. Everything must be
write-temp plus atomic rename, which is already how `memostore.jl` writes blobs.

**Metadata storms.** 10,000 shards each writing a status file, polled individually, is 10,000
stat calls per poll against a metadata server shared by the entire institution. This constraint
dictates the reporting design.

### 6.2 Reporting channels

| channel | cost | granularity | role |
|---|---|---|---|
| `squeue` / `qstat` | one call, all elements | pending/running/done per chunk | always-available baseline |
| per-chunk status file | one directory listing | shards done within a chunk | the workhorse |
| CAS blob presence | free, already written | authoritative completion | truth for "done" |
| `slurm-<jobid>_<idx>.out` | on demand | stdout, tracebacks | opening a failed shard |
| live callback to hub | n/a | realtime | rejected, see below |

A ZMQ dial-back from tasks to the hub reintroduces the mesh model: compute nodes usually cannot
reach a laptop, and the hub may be down for a day. It can be an opportunistic enhancement. It
cannot be the mechanism.

### 6.3 The status protocol

Each chunk writes one small JSON status by temp-plus-rename into `<sweep>/status/<chunkid>.json`:

```json
{"chunk": 17, "done": 37, "total": 50, "current": "beta=1.2", "failed": [4712], "ts": 1787000000}
```

The hub does **one directory listing per poll**, which returns every chunk's mtime and size in a
single round trip regardless of file count, then fetches only what changed. 200 chunks, one
`ls`, a handful of small reads.

Polling is adaptive: frequent while results are landing, slow when idle, and zero when no
browser has the notebook open. This matters when a sweep runs for days.

### 6.4 Getting results back

The hub cannot see the cluster filesystem. Slate runs on a laptop; scratch is mounted on the
cluster. So there are two CAS roots, cluster-side and laptop-side, and results must cross.

Existing transfer machinery assumes a live worker with a ZMQ data channel. Batch has no live
worker. The batch path is a plain **pull by key over ssh/rsync**: no ports, no CURVE, no tunnel.

It must be lazy, which forces a first-class distinction:

- **Return values**: small, fetched eagerly, drive the live table and plot. 4,000 numbers is
  nothing.
- **Artifacts**: files, potentially huge, stay on the cluster as references carrying size and
  checksum. Fetched only when a cell asks for them.

This split is what makes the flagship demo work. The plot fills from kilobytes while terabytes
stay in place.

## 7. API sketch

A plain function rather than a macro, so it works identically with no cluster attached:

```julia
grid = paramgrid(β = 0.1:0.1:2.0, seed = 1:100, model = [:ising, :potts])   # lazy, 4000 combos

results = slate_map(grid; region = "hpc",
                    resources = (cpus = 4, mem = "16G", walltime = "30m")) do p
    simulate(p.model, p.β; seed = p.seed)
end
```

With no `region`, it runs locally on threads. The same notebook develops on a laptop over 10
parameters and runs 10,000 on the cluster by changing one tag.

`results` is a `ShardedResult`: table-like, usable while incomplete, carrying per-shard status.
`failed(results)` returns the parameters and stderr of failures. Re-running the cell retries only
failures.

## 8. Chunking

One shard per array element is wrong. Julia startup plus package load can dwarf a 30-second
shard, and schedulers handle 10^5-element arrays badly. Pack M shards per element.

Chunk size is a real tradeoff. Long enough to amortize startup, short enough to fit **backfill**
windows, where a 20-minute job is scheduled in minutes while a 6-hour job waits a day. Slate can
measure one shard locally (the DAG already times cells) and size chunks automatically. This can
change queue wait from hours to minutes.

The existing PackageCompiler sysimage path (`_kickoff_sysimage_build!`) attacks the same startup
cost from the other side and should be reused.

A day-long sweep should contain no day-long job. Short chunks are backfill-friendly,
preemption-tolerant, and every finished chunk is durable progress. A single 24-hour job killed
at hour 23 loses everything.

## 9. Sharp edges

**Closure captures.** `do p ... end` captures notebook variables, which must be serialized into
the task descriptor and content-addressed. This is the same problem region boundary transfer
solves, but an accidental capture of a 2 GB DataFrame now ships to 4,000 tasks instead of one
region. The existing transfer preview and cost gate is the mitigation, applied per fan-out.
Treat this as a primary design concern.

**Live-updating table thrash.** A naive implementation re-runs downstream cells once per landing
shard, which for 4,000 shards is 4,000 re-runs. Updates must be coalesced and debounced from the
start.

**Site variance.** Partition names, whether compute nodes are reachable, whether `$HOME` is
shared, Lmod versus bare environments, `sacct` availability. Anything built here needs per-site
configuration, and a `prologue` field (arbitrary shell before Julia) plus `depot_root` covers
most of it.

**Scheduler etiquette.** Submitting thousands of jobs, hammering a metadata server, or
precompiling on a login node are all ways to get a user's account restricted. The defaults must
be conservative.

## 10. Build order

Each step is independently reviewable.

1. This document.
2. **Docker SLURM harness**: slurmctld, two slurmd, munge, a shared volume as `/scratch`.
   Nothing can be tested without it.
3. **`SlateTask` runner**: the stateless CAS-in, CAS-out entry point. Testable locally under
   `ExecLauncher` with no scheduler involved.
4. **Reconciler and shard keys**, against a fake launcher. Mostly pure logic, no cluster needed.
5. **`SlurmLauncher`** with batched poll, wired to the harness.
6. **`slate_map` and shard memoization**, wired to the DAG and progress UI.
7. **Live-updating `ShardedResult`** with coalesced downstream invalidation.
8. **Region on a compute node** (persistent kernel for interactive GPU work). Nearly free once
   step 5 exists. `Tunnel.forwards` (`src/remote.jl:216`) hardcodes `127.0.0.1` as the forward's
   remote side; making that a field turns `ssh -L lport:127.0.0.1:rport login` into
   `ssh -L lport:node042:rport login`. The login node reaches the compute node, so no ProxyJump
   is needed.

## 11. Open questions

- Should a shard be able to be a shell command rather than a Julia closure? This unlocks "run
  this Fortran binary 10,000 times", which is a large fraction of real cluster use, at the cost
  of a second execution model (argument templating, exit codes, stdout capture). Deferred, not
  rejected.
- Artifact garbage collection. Scratch filesystems are purged on a schedule. What happens to a
  notebook whose artifacts were reaped?
- Cross-user or cross-notebook CAS sharing on a cluster. Two users running the same sweep should
  arguably share results, which raises permissions questions.
- Whether the provision job should also build a sysimage per depot class.
