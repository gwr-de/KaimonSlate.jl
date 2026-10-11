# Cell profiler: design

A profile mode for one cell, beside the debugger: compile the cell's code without running it, run
it under the sampling profiler in whichever worker runs the cell (local or a region), and show where
the time went as a flame graph built out of the code itself. A profiler specialist works from the
same data.

Temporary working notes, not for check-in. Written 2026-10-06.

---

## 1. The flame graph is the code

A conventional flame graph is keyed by function. This one is keyed by **source line**, and its top
row is the cell.

- **Rooted at the cell, one bar per line.** The first row under the root is the cell's own lines in
  source order, each as wide as its inclusive time and labelled with its line number and text. The
  graph starts where the reader's code starts, not at `eval` or the task entry.
- **Every function is a function bar over its lines.** A frame in Julia's profile data is already
  (file, line, function), so each function bar is split, one row down, into the lines inside it
  that the time passed through, and the callees hang under those lines. Reading down a column is
  reading a call path one line at a time: cell line 7 → `step!` line 31 → `flux` line 12 →
  `getindex`.
- **The code pane is the selected column.** Selecting a bar shows that function's source in the
  pane with per-line heat in the gutter, in the bar colours, and re-roots the graph there. The
  breadcrumb above both is the path from the cell, and Back walks up it.
- **Two-way.** Hovering a bar highlights its line in the pane. Hovering or selecting a line in the
  pane highlights every bar that line produced. A line called from several places (a hot helper)
  shows all of them at once.
- **Other people's code folds.** Frames in Base, the stdlibs and packages collapse into one bar per
  package until expanded, coloured by package, so the reader's own code (cell and dev'd packages)
  stands out. A folded bar still carries its marks.
- **Marks on bars and lines:** runtime dispatch (time under `jl_apply_generic`), GC, compilation
  (inference and codegen frames), and, in allocation mode, bytes. The same glyph sits on the bar
  and in the gutter of the line it came from.
- **Diff.** Against an earlier profile of the same cell, bars and gutters colour by change, and the
  hot-lines table gets a before/after column.

Flame chart (time on the x axis, one row per thread) is a second view of the same samples, on the
telemetry time axis so GC pauses and memory line up under it. Same linking to the pane.

## 2. Workflow

1. **Enter profile mode** on a cell (cell menu, like Debug). A dock opens with the cell's code.
2. **Prepare** compiles without running (§3) and runs the static check. The dock reports what was
   compiled and marks lines JET flags before anything runs. Prepare is shown as out of date when an
   upstream cell changes the types the cell reads.
3. **Run** runs the cell exactly as a normal run would (same eval path, memo bypassed) under the
   profiler, keeping the cell's own samples (§4). Modes: CPU (default), wall time, allocations,
   and GPU timings on a node with CUDA.
4. **Read** the flame graph and the hot-lines table, drill in, open `code_warntype` for any frame at
   the argument types the profile saw.
5. **Iterate:** edit, Run again, compare with the previous profile.

## 3. Compiling without running

`precompile(f, argtypes)` infers and compiles `f` and everything inference resolves from it, and
runs nothing. A cell is top-level code, so prepare makes a function of it:

- The cell's global reads are known (`deps.jl`, `cell.reads`). The body becomes
  `function __slate_prof_<id>(r1, r2, …) <body> end`, defined in the notebook namespace so every
  other name resolves as it does in the cell, with the reads as arguments.
- Argument types are the types of the current values in the worker's namespace, so
  `precompile(f, Tuple{typeof(v1), …})` compiles what the real run will call.
- Assignments in the body become locals of the wrapper. It is never called, so that is harmless.
- Reported: compile time (`Base.cumulative_compile_time_ns` delta) and the number of methods
  compiled.

What it cannot compile is what is dispatched at runtime. Whatever compiles during the real run is
measured and marked, and is itself a finding: "line 7 compiled for 1.8 s at runtime" means line 7
is type unstable.

**Static check.** JET's `report_opt` on the wrapper at those argument types lists runtime dispatch
sites and boxed captures without running anything. JET is heavy, so it is loaded only in profile
mode, into the worker from a side environment (open question, §8).

**Warm-up run** stays as an option for cheap cells: the most thorough, but not for a long cluster
cell or one with side effects.

**The wrapper is not what is profiled.** The real run is top-level code over non-const globals and
that is what is measured. When a cell has a hot loop at top level, that shows as time on the cell's
own line with dispatch marks, and the analysis says to move it into a function.

## 4. Capture, in the worker

All of it runs in the worker that runs the cell, so a region cell profiles on its node and only a
compact result crosses the wire.

- `Profile.init` with a buffer sized for the expected run and a 1 ms delay (configurable).
- The run goes through the normal `__slate_eval` path inside `Profile.@profile` (or
  `Profile.@profile_walltime`, or `Profile.Allocs.@profile`).
- `Profile.fetch(include_meta = true)` gives each sample's thread, task and time. Keeping only the
  cell's task loses work it spreads over threads (`Threads.@threads` children are other tasks), so
  the filter is the reverse: drop samples from tasks known to be something else (other running
  cells, the gate, telemetry, the blob channel) inside the run's time window. The dock reports how
  many samples were dropped and why.
- Compile time and GC during the run come from the counters and the GC event hook already in the
  worker.

**What ships to the hub:**

- The tree: nodes `{id, parent, file, line, func, pkg, self, total, marks}` keyed by
  (file, line, func) along the path, pruned below a share of the total into "other" nodes.
- Per-(file, line) totals for every file in the profile, for gutter heat in any source view.
- Run facts: duration, samples kept and dropped, compile and GC time, mode.

The worker keeps the full data until the next profile, so drill-down queries (an unpruned subtree,
a frame's source, `code_warntype`) are answered there. Source text comes from the worker's own
files and depot, so a region shows the versions it actually ran.

## 5. Hub

- Worker tools: `__slate_profile_prepare`, `__slate_profile_run`, `__slate_profile_subtree`,
  `__slate_profile_source`, `__slate_profile_warntype`, `__slate_profile_jet`.
- Routes under `/api/{id}/profile/…`, mirroring the debugger's (`server_debug.jl`).
- Profiles saved per cell and run under the cache home, linked from the run record, so the
  telemetry timeline's run bar opens its profile and two runs can be compared.

## 6. UI

`profiler.js`, a dock built like `debugger.js`:

- Left, the code pane: a read-only CM6 view with the heat gutter and marks; the cell, or the
  selected frame's function.
- Right (or below on a narrow page), the flame graph on a canvas, with the flame chart as a tab.
- Above both, the breadcrumb.
- Below, the hot-lines table (self, inclusive, marks, change) and the run facts.
- Toolbar: Prepare, Run with its mode, Compare with, and the specialist.

The renderer is our own canvas code: it has to draw line rows inside function bars, fold packages
and link to the pane, which no existing renderer does, and ECharts does not handle trees of this size.

## 7. The profiler specialist

A `Specialist` registered in `server_specialists.jl`, which was written with this second role in
mind. Verbs:

- `prof_summary`: hot paths, self-time leaders, marks, compile and GC share, as text
- `prof_frame` / `prof_source`: a frame's subtree and its source with heat
- `prof_warntype` / `prof_jet`: inference at the observed argument types
- `prof_run`: run the profile again
- `prof_compare`: compare two profiles
- `edit_cell`, and an edit proposal for a dev'd package, accepted by the person

It asks the person when the intent of the code matters (a type that looks unstable on purpose) and
signs off with findings, as the debugger specialist does.

## 8. Open questions

- **JET in the worker:** a side environment pushed with the worker payload, or installed on first
  use of profile mode. It must not enter the notebook's own environment.
- **Wrapper for cells that define things** (functions, structs, `using`): those lines are skipped by
  prepare and still run normally.
- **Sample buffer** for long runs: size from the cell's last run time, and say when it overflowed.
- **Sweeps:** each array task could profile itself and write its tree to the store. Later.

## 9. Build order

1. Prepare (wrapper + precompile + compile report) and Run (CPU mode, sample filter, tree) in the
   worker, with tests that run locally. The dock with the code pane, heat gutter and the line-rooted
   flame graph, linked both ways. Drill into callee source. Local and remote.
2. Saved profiles per run, the run-bar link, compare. Flame chart on the telemetry axis.
3. JET check and `code_warntype` at observed types. The profiler specialist.
4. Wall-time, allocation and GPU modes.

---

## Kernel metrics (DRAM throughput, occupancy): notes, 2026-10-09

Not built. In-process, through CUPTI's range profiler (CUDA 12.6+): `cupti_range_profiler.h` +
`cupti_profiler_host.h`, both in libcupti (no separate NVPW calls). CUPTI.jl binds only the legacy
`cuptiProfiler*` calls, so these need our own `ccall`s against `CUPTI.libcupti`.

- Host side: `cuptiProfilerHostInitialize` (chip name from the device), `ConfigAddMetrics` with a small
  set (`dram__throughput.avg.pct_of_peak_sustained_elapsed`, `dram__bytes.sum.per_second`,
  `sm__warps_active.avg.pct_of_peak_sustained_active`), `GetConfigImage`, `GetNumOfPasses`.
- Target side: `cuptiRangeProfilerEnable` on the context, `SetConfig` with `CUPTI_AutoRange` +
  kernel replay (each kernel a range, replayed once per pass), counter data image sized by
  `GetCounterDataSize`, `Start`/`Stop` around the run, `DecodeData`, then
  `EvaluateToGpuValues` per range.
- Kernel replay serialises and replays every kernel: a separate opt-in run ("Analyze kernels"),
  capped at the first N launches per kernel name, never the timing run.
- Available on Perlmutter's CUDA 13.2 (headers under hpc_sdk .../extras/CUPTI/include); `ncu` is
  there too but needs to launch the process, which a live worker cannot be.
