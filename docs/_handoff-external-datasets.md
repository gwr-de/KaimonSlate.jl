# Adopting already-written data files, and the sweep result model

**Branch:** `feat/hpc-cluster` · **Worktree:** `.claude/worktrees/feat+batch-fabric`, hub `slatebf` on :8901
**Built:** 2026-09-11

## Adoption — done

`adopt(path)` / `adopt(paths)` brings a file the unit wrote into the dataset layer. Chosen design of
the three (read-on-region / ingest-on-adoption / index-the-file's-chunk-map): **ingest**, because the
read happens on the node that already holds the file, so the second copy is local disk and what
reaches the notebook is an ordinary index — the whole read side is unchanged code.

An adopted file indexes as a **group**: `keys(ds)` names the variables, `ds[:sst]` returns a normal
`:array`/`:table` Dataset, `Sweep.attributes` / `Sweep.dimensions` carry the metadata. Adapters go
through `register_dataset_adapter!`; HDF5 and NCDatasets are soft-detected built-ins because the
batch payload is four stdlib-only files.

Validated on the real SLURM cluster: `cubes` 4/4 (HDF5, 48³ Float32, `timestep` attribute) and
`grids` 4/4 (NetCDF, 180×90, dims `lon`/`lat`, `units=degC`).

Demo: `SlateCluster.jl/notebooks/adoption_demo.jl`, written in ClimaAnalysis's shape —
`<short_name>_<reduction>_<period>.nc`, CF dims with coordinate variables, `units`/`long_name`.

## The result model — BUILT

`r.dataset` is the single view; `r.table`, `Sweep.table` and the `Sweep.unified` prototype are
deleted. The old split was about STORAGE (chunked vs inlined), which no reader should have to know.

`DatasetPart` carries a `backend` and `part_rows(p, range)` is the only function that branches on it:

  - `:indexed` — chunked Arrow, a raw array, or an adopted file's group, read through the stored index
  - `:inline` — the rows rode the manifest record, because they were small enough to
  - `:none` — no addressable rows: stored whole, failed, or not run

`_dataset_of` emits a part per GRID POINT in grid order, so a hole sits where the work is — the
property `table` had. That is a table-space notion only: `:array` and `:group` datasets keep just
their landed parts, so `ds[k]` and `ds[:sst]` are unchanged. Parameters and facts are `BlockColumn`s
off the parts, stored once per unit; facts are off the default projection and come back when `select`
names them. `scan` walks the row space (`_scan_blocks`) rather than the chunk list, so inline rows
are not invisible to it.

Separating SHAPE (rows / array / group / opaque) from STORAGE (inline / arrow / raw / jls) is the
idea the whole thing rests on. Arrow is one backend for the rows shape, not the answer everywhere:
`raw` stays for arrays because a slice is `offset + i*elsize`, which is what lets a remote read be
`dd` over ssh with nothing running on the far side.

Still worth doing: `_dataset_of` and `_rows` each read every manifest, so a refresh parses them
twice. Carrying the stored index on the row would halve it.

`data=` now takes `auto` / `arrow` / `whole` (`lazy`/`eager` removed outright) and is a select in the
sweep config panel.

## Still open

- **Manifest batching.** One manifest per SHARD means a million-unit sweep is a million files in one
  flat directory (`manifest_path` is flat; blobs are sharded). Per-CHUNK manifests written
  incrementally by the one process that owns the chunk is the fix. Hard part: shard keys are
  content-addressed, so cross-run reuse must survive the key no longer being the filename. Also:
  params are one blob per unit and could be inlined into the chunk descriptor. The other half of
  the old motivation is gone: one-row-per-unit is already a real dataset, through the `:inline`
  backend, without touching how manifests are stored.
- **Read on the region** for variables too large to ingest.
- **Run pinning** — addressing a prior run so an edited grid does not strand its results.
- **Cluster panel** — `created` is computed and dropped on the wire; `sweep_bytes` counts only
  OUTPUT so a `ready` sweep reads `0 B` while holding a blob per parameter point; no release button
  (the primitive exists: `Sweep.forget_run!`).

## Developing Slate re-keys sweeps

`env_key(target)` is a SOURCE-INCLUSIVE fingerprint of the task environment's parent project, and a
notebook env that `dev`s this checkout therefore carries KaimonSlate's own source. So editing the
runner re-keys every sweep in every demo notebook and orphans its results. That is correct — an edit
the fingerprint cannot see is an edit the compute nodes never get — and a user never hits it. While
developing the fabric it means resubmitting after each source change, and the orphaned runs are
ARMED with results, so the at-most-one-unarmed rule does not collect them. They accumulate.

## Traps worth keeping

- A shard module holds only what is injected. `datadir()`/`@sfile`/`adopt` had to be put there.
- `:macrocall`'s second argument is positional — stripping the LineNumberNode silently dropped every
  macro call's arguments in a sweep body.
- Deparsing does not round-trip (a filtered comprehension returns as `$(Expr(:filter, …))`), so
  shipped source must be verbatim, sliced by line span.
- A throwing `text/html` method does not surface — the notebook falls back to the next MIME, so the
  card silently becomes text.
- `env = :parent` does NOT replay the notebook's package adds; a body needing a package names it.
- Arming records that work was wanted, not that THIS call should start it. Reading the armed marker
  in `run_sweep` made a cell run resubmit, and a worker restart re-runs every cell.
- `run_tests` in this worktree needs `project_path` pointing AT the worktree. Without it the runner
  includes the main checkout's `test/runtests.jl`, which has none of the batch-fabric test files —
  and reports PASSED. Any green run recorded here without that argument proves nothing.

## Scaffolding to remove

- region `xfertest` → `slate-remote`, preload `/tmp/slate-xfer-demo`
- `/tmp/slate-xfer-demo`, `SlateTest.jl/notebooks/what_travels.jl`, `SlateTest.jl/.slateignore`
- `.kaimonslate-dev/config/slate.json` has `transfer_warn_mb = 5.0` (default is 100)
- `kaimon.toml` namespace is `slatebf` — marked DO NOT COMMIT, revert before merging
