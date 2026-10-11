# Sweeps on a region's node, and making long work discoverable

Status: proposal, 2026-10-08. Nothing here is built.

## The gap

Slate has two ways to run work on a cluster, and nothing in between.

| | Batch sweep | Interactive region |
| --- | --- | --- |
| A cell says | `#%% job cluster=pm` | `#%% code region=pm_gpu1_plain` |
| Runs as | scheduler jobs, one environment test first | a live worker on one held node |
| Results | a store, resumable, keyed by content | values in the namespace, gone with the worker |

The common case that falls between them: a study of tens to a few hundred evaluations, each a minute
or so on a GPU, on a node the notebook already holds. As a region cell it runs for an hour with no
persistence, so a recompute or a hub restart loses it. As a batch sweep it waits in the queue for new
allocations and pays a fresh Julia start, package load and GPU compile per job.

What exists does not combine. A job cell's `cluster=` names a machine, and a scheduler machine submits
new jobs. The `here` machine runs local processes, but its store is a path on the hub's machine, so a
job cell tagged `region=pm_gpu1_plain` with `cluster=here` would try to write that path on the node.

### The case that prompted this

`StellaratorEdge.jl/notebooks/reactant_ml.jl`, cell `edge_cost`, on `pm_gpu1_plain` (one A100):

- One exact edge evaluation (potential grid, magnetic axis, last closed surface, enclosed flux) takes
  52 to 62 s. Grid 2.6 s, axis 0.2 s, flux about 0; the rest is field-line tracing.
- Tracing time follows transits, not lines: 512 lines cost the same as 2048, so the GPU is mostly idle.
- Fewer transits is not an option: at 300 the orbit classification calls tori chaotic, and the edge
  moves inward every time, with the flux 6% to 77% low.
- Loading this notebook's packages in a fresh worker takes about 24 s, before GPU compilation.

A surrogate needs hundreds of these. Run as a plain cell, the sweep that measured them ran over 9
minutes with no persistence and a progress meter that restarted inside its loop.

## Proposal: a sweep whose target is a region

A job cell names a region instead of a machine, either `cluster=region:pm_gpu1_plain`, or
`region=pm_gpu1_plain` with no `cluster=`. Its units run on the node that region holds.

1. **Environment.** Units use the region's environment, already built and precompiled for that node
   type by the region's prepare. Nothing new is built, and no test job is needed: the region's worker
   already loaded it there.
2. **Store.** A store under the region's data root on scratch, written the way a cluster store is:
   per-unit manifests, keyed by content. It outlives the allocation, the worker and the hub, and the
   usual reconcile applies: a re-run submits only what has not landed.
3. **Warm processes.** Units run in long-lived task processes on the node, many units per process, so
   loading and GPU compilation are paid once per process rather than per unit. For GPU work the
   default is one process per GPU. The notebook's own worker stays free for interactive cells.
4. **Which node.** Resolved at Submit from the region's current allocation. With none held, the sweep
   asks the region for one, as a region cell does, and the card says it is waiting for a node.
5. **Allocation ending.** Units still running when the walltime or an idle release ends the
   allocation are not lost: they have no manifest, so the next Submit runs them again.
6. **Submit stays a person's action.** The card states units, destination and an estimated time
   before it spends anything, as it does now.

### Settled (2026-10-10)

- The cluster path stays as it is. A job cell names a machine (`cluster=`) for scheduler batch jobs, or a
  region (`region=`) for its held node; the `@sweep` cell, the per-unit store and the reconcile are the same.
- Units run in hub-managed gate workers on the region's node, one per GPU, with the region's environment,
  sync and Revise.
- Units in flight when an edit re-keys the sweep finish under the key they started with; the next Submit
  runs what the new key lacks.
- A running region sweep counts as activity for idle release.

### Open questions (before 2026-10-10)

- Whether a task process should be a plain `slatetask` runner started on the node over the existing
  routed ssh, or a second gate worker the hub manages like the region's own. The first reuses the
  batch path; the second gets the worker's sync and Revise, which matters if the body's code is being
  edited while units run.
- How live edits to developed packages interact with a running sweep. Today an edit to code the body
  reaches re-keys the sweep, which is right for results but means units in flight are working on the
  previous key.
- Whether idle release should count a running region sweep as activity (it should).

## Making long work discoverable

An agent working in a notebook did not find any of this by itself. Where it went wrong:

1. `slate_api("job cell")` and `("@sweep")` describe what a job cell is, but nothing points to them
   from where a long computation starts: the region docs, `run`, `add_cell`, or the progress tools.
   The agent wrote a 9-minute plain cell.
2. Nothing says how a job cell relates to a region, or whether a sweep can use a held node. It took
   reading `src/sweep.jl` and `docs/src/clusters.md` to learn that it cannot.
3. Nothing says Submit is a person's action. The agent tried `Sweep.resume!`, which only undoes a
   cancel and returned with the sweep still `ready`.
4. `search_docs` returned nothing useful for "sweep on a region's node" or for submitting.
5. The consequence of editing code a sweep uses (a new sweep at 0 of N, the old results hidden until
   the code matches again) is correct and documented in one sentence, but nothing warns at the moment
   of the edit.

### Recommendations

1. **One entry for long work**, linked from `job cell`, `@sweep`, `regions`, `remote`, `run` and
   `add_cell`: when work belongs in a job cell (over about a minute, not interactive, results worth
   keeping), which machine kinds exist (`here`, scheduler, and region once built), and how to estimate
   before submitting.
2. **State that Submit is a person's action**, and what an agent does instead: pilot a few points,
   estimate units × time and destination, and ask.
3. **Progress belongs to the whole job.** The long-work entry should show one `@withprogress` meter
   over total units, naming the current one, with library meters inside it folded in rather than
   restarting it.
4. **Warn when an edit re-keys a sweep.** `edit_cell`'s reply, and the card, say "this edit starts a
   new sweep; N finished units stay stored under the previous code".
5. **Index these for `search_docs`** with the phrasings an agent uses: "run this as a background
   job", "persist results", "resume", "sweep on my node", "submit".
6. **Agent submission with approval, later.** Once agents use the system reliably: an agent proposes
   units, destination and estimated cost, and a person approves on the card. The reason it is a
   person's action today is that agents have not weighed what long jobs cost, and fast code churn
   re-runs expensive work that was not prepared well.

## Observed behaviour, for reference

From `mbse/scratch/sweep_practice/sweep_practice.jl`, 12 two-second units on `here`:

- Running the job cell plans the sweep and reports `ready`. Nothing runs until Submit.
- Re-running the cell after it finished keeps the same sweep and recomputes nothing.
- Adding a value to the grid keeps the sweep; only the new points wait to run.
- Editing the helper the body calls starts a new sweep at 0 of N. Reverting the edit brings back the
  previous sweep and its results.

## As built (2026-10-10)

- `clusters_resolved()` adds `region:<name>` beside the machines: kind `region`, store
  `<machine root_remote>/regions/<name>` (else `<data_root>/sweeps`). A job cell with `region=<name>` and no
  `cluster=` resolves to it (`Sweep.resolve_target`); the job cell itself runs on the notebook's own kernel
  (`_cell_region` ignores a job cell's tag).
- `BatchLauncher.RegionLauncher`: `submit!` writes `queue/<name>.want` (region, parent env, chunks); a task
  worker running a chunk keeps `queue/<chunk>.claim` fresh and writes `<chunk>.done` when it returns
  (`BatchSweep.run_queued_chunk`); `poll` reads the queue in one command; `cancel!` removes the want.
- The hub's driver (`src/region_sweeps.jl`, on the supervisor tick, 10 s / 30 s per region) reads each queue,
  asks for the node when none is held (not after a release that came later than the work), starts one
  task worker per GPU (`CUDA_VISIBLE_DEVICES`), hands out chunks (`__slate_run_chunk`), stops a worker whose
  chunk is no longer wanted, removes finished wants, and stops idle workers after 2 minutes.
