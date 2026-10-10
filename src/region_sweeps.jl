# ── Region sweeps: the hub runs their chunks on a region's node ─────────────────────────────────
# A job cell with `region=<name>` and no `cluster=` queues its chunks in the region's store
# (`Sweep.RegionLauncher`). Here the hub takes them: on the node the region holds it starts task
# workers, one per GPU (one on a CPU node), each pinned to its GPU and started in the notebook's
# environment like the region's own worker, so it is synced and Revise'd the same way. Each worker runs
# one chunk at a time and many chunks over its life, so loading and compilation are paid once.
#
# The store is the only channel between the notebook and this driver. The card submits by writing a
# want, reads progress from the store and the claims, and cancels by removing the want; the driver
# reads the queue, starts and stops workers, and removes a want once each of its chunks has returned.
# A hub that restarts finds the queue where it was, and a worker that dies leaves a claim that goes
# stale, so its chunk is taken again.

mutable struct _TaskWorker
    kernel::ReportEngine.GateKernel
    slot::Int                       # the GPU it is pinned to; -1 on a CPU node
    chunk::String                   # the chunk it is running ("" = free)
end

mutable struct _RegionSweeper
    region::String
    parent::String                  # the notebook environment its workers start in
    host::String                    # the node they run on; a new node means new workers
    job::String
    workers::Vector{_TaskWorker}
    idle_since::Float64             # when the queue last had nothing for these workers (0 = busy)
end

const _SWEEPERS = Dict{Tuple{String,String},_RegionSweeper}()   # (region, parent) → its workers
const _SWEEPERS_LOCK = ReentrantLock()

"How long task workers stay up with nothing queued for them, in seconds."
const _SWEEP_IDLE_STOP = 120.0

# Task workers for a region: one per GPU the region asks for, else one.
function _sweep_slots(r)
    m = match(r"(\d+)\s*$", r.gpus)
    n = m === nothing ? 0 : parse(Int, m.captures[1])
    return n > 0 ? collect(0:n-1) : [-1]
end

# The worker for `slot`: the region's target for this environment, its shell pinning the GPU.
function _task_worker(r, parent::AbstractString, slot::Int)
    t = ReportEngine._region_target(r; origin_env = parent)
    if slot >= 0
        pin = "export CUDA_VISIBLE_DEVICES=$slot"
        t = ReportEngine.RemoteTarget(t.ssh_host; transport = t.transport, project = t.project, port = t.port,
            stream_port = t.stream_port, origin_env = t.origin_env, datadir = t.datadir,
            cache_root = t.cache_root, region = t.region, sysimage = t.sysimage, curve = t.curve, job = t.job,
            depot = t.depot, setup = isempty(t.setup) ? pin * "; " : rstrip(t.setup, [';', ' ']) * "; " * pin * "; ")
    end
    label = basename(rstrip(String(parent), '/')) * "#" * r.name * "#task" * (slot < 0 ? "0" : string(slot))
    k = ReportEngine.GateKernel(t.project; parent = String(parent), target = t,
                                threads = ReportEngine.region_threads(r), label = label)
    k.conn, k.tunnel = ReportEngine.spawn_and_connect_remote!(k, t, String(parent))
    ReportEngine._rlog("region[$(r.name)]: task worker $label up on $(t.ssh_host)")
    w = _TaskWorker(k, slot, "")
    # Reattached to a worker a previous hub started: it may be part way through a chunk already.
    st = try; ReportEngine._tool(k, "__slate_chunk_status", Dict{String,Any}(); timeout = 15.0); catch; nothing; end
    (st !== nothing && get(st, "state", "") == "running") && (w.chunk = String(st["chunk"]))
    return w
end

_stop_task_worker!(w::_TaskWorker) =
    (try; ReportEngine.shutdown!(w.kernel; kill_remote = true, wait = false); catch; end; nothing)

function _stop_sweeper!(s::_RegionSweeper, why::AbstractString)
    isempty(s.workers) && return nothing
    ReportEngine._rlog("region[$(s.region)]: stopping $(length(s.workers)) task worker(s) — $why")
    foreach(_stop_task_worker!, s.workers)
    empty!(s.workers)
    return nothing
end

"""
    _drive_region_sweeps!(h)

One pass over every region with a store for sweeps: read its queue, and start, feed or stop its task
workers to match. Runs on the supervisor's tick, one pass per region at a time (`_region_work!`).
"""
function _drive_region_sweeps!(h)
    now = time()
    for e in ReportEngine.clusters_resolved()
        String(get(e, "kind", "")) == "region" || continue
        name = String(get(e, "_region", ""))
        r = ReportEngine.region_get(name)
        r === nothing && continue
        # Every 10 s while the region has task workers, every 30 s otherwise: each pass is one
        # command on the login host, and an idle store costs only that.
        active = lock(_SWEEPERS_LOCK) do; any(s -> s.region == name && !isempty(s.workers), values(_SWEEPERS)); end
        now - get(_SWEEP_POLLED, name, 0.0) < (active ? 10.0 : 30.0) && continue
        store = String(get(e, "root_remote", ""))
        _region_work!(name, :sweep) do
            t0 = time()
            _drive_region_sweep!(r, store)
            # A pass is a command on the login host and a status call per worker. One that takes long
            # holds up every chunk behind it, so it is worth a line in the log.
            (dt = time() - t0) > 30 && ReportEngine._rlog("region[$name]: a sweep pass took $(round(Int, dt))s")
        end && (_SWEEP_POLLED[name] = now)
    end
    return nothing
end
const _SWEEP_POLLED = Dict{String,Float64}()

function _drive_region_sweep!(r, store::AbstractString)
    BL = ReportEngine.Sweep.BatchLauncher
    runner = (hh, sc) -> ReportEngine.Sweep.run_there(hh, sc)
    ReportEngine.Sweep.connected(r.host) || return nothing        # nobody signed in: nothing to read
    q = BL.read_queue(runner, r.host, store)
    wanted = Set{String}(c for w in values(q.wants) for c in w.chunks)
    mine = lock(_SWEEPERS_LOCK) do; [s for s in values(_SWEEPERS) if s.region == r.name]; end
    foreach(s -> _check_workers!(s, r, wanted), mine)
    byparent = Dict{String,Vector{String}}()    # environment → its wants
    for (n, w) in q.wants
        push!(get!(Vector{String}, byparent, w.parent), n)
    end
    # Workers with nothing queued for their environment stop after a while, not at once: the next
    # submission of an interactive session usually follows soon, and starting a worker is the slow part.
    for s in mine
        haskey(byparent, s.parent) && continue
        any(w -> !isempty(w.chunk), s.workers) && continue
        s.idle_since == 0 && (s.idle_since = time())
        time() - s.idle_since > _SWEEP_IDLE_STOP && _stop_sweeper!(s, "nothing queued")
    end
    isempty(q.wants) && return nothing
    _region_used!(r.name)                       # queued work keeps the node, as a running cell does
    host, job = ReportEngine.region_where(r)
    if r.scheduler !== :none && isempty(job)
        # The sweep asks for the node as a region cell does, unless the region's request was
        # withdrawn after the work was queued: a release is not undone by work that was waiting on it.
        queued_at = time() - minimum(w.age for w in values(q.wants))
        withdrawn = lock(_PLACING_LOCK) do; get(_WITHDRAWN_AT, r.name, 0.0); end
        withdrawn > queued_at || _place_in_background!(r.name)
        return nothing
    end
    for (parent, names) in byparent
        s = lock(_SWEEPERS_LOCK) do
            get!(_SWEEPERS, (r.name, parent)) do
                _RegionSweeper(r.name, parent, host, job, _TaskWorker[], 0.0)
            end
        end
        (s.host, s.job) == (host, job) || (_stop_sweeper!(s, "the region has a new node"); s.host = host; s.job = job)
        _feed_sweeper!(s, r, store, q, names, runner)
    end
    return nothing
end

# What each worker is doing. A worker whose chunk is no longer wanted was cancelled with its want: it
# is stopped, and the units it landed stay landed. One that does not answer is gone.
function _check_workers!(s::_RegionSweeper, r, wanted::Set{String})
    for w in copy(s.workers)
        st = try
            ReportEngine._tool(w.kernel, "__slate_chunk_status", Dict{String,Any}(); timeout = 15.0)
        catch e
            ReportEngine._rlog("region[$(r.name)]: task worker $(w.kernel.label) did not answer — " *
                               first(sprint(showerror, e), 120))
            nothing
        end
        if st === nothing
            _stop_task_worker!(w); filter!(!=(w), s.workers); continue
        end
        state = String(get(st, "state", "idle"))
        if state == "running" && !(w.chunk in wanted)
            ReportEngine._rlog("region[$(r.name)]: chunk $(w.chunk) is no longer wanted — stopping its worker")
            _stop_task_worker!(w); filter!(!=(w), s.workers); continue
        end
        if state in ("finished", "failed")
            state == "failed" && ReportEngine._rlog("region[$(r.name)]: chunk $(w.chunk) failed: " *
                                                    String(get(st, "error", "")))
            w.chunk = ""
        elseif state == "idle"
            w.chunk = ""
        end
    end
    return nothing
end

function _feed_sweeper!(s::_RegionSweeper, r, store, q, names, runner)
    BL = ReportEngine.Sweep.BatchLauncher
    # Chunks to give out: wanted, not returned, and not held by a live claim.
    taken = Set{String}(w.chunk for w in s.workers if !isempty(w.chunk))
    free_claim(c) = (cl = get(q.claims, c, nothing); cl === nothing || cl.age >= BL.CLAIM_STALE_S)
    todo = [c for n in sort!(collect(names)) for c in q.wants[n].chunks
            if !(c in q.done) && !(c in taken) && free_claim(c)]
    # A want whose chunks have all returned is finished: its card reads the store for what landed.
    finished = [n for n in names if all(in(q.done), q.wants[n].chunks)]
    isempty(finished) || runner(r.host, "rm -f " * join((BL._shq(joinpath(store, "queue", f))
        for n in finished for f in [n * ".want"; [c * ".done" for c in q.wants[n].chunks]]), ' '))
    s.idle_since = isempty(todo) && isempty(taken) ? (s.idle_since == 0 ? time() : s.idle_since) : 0.0
    isempty(todo) && return nothing
    # Start the workers the work can use, then hand each free one a chunk.
    have = Set(w.slot for w in s.workers)
    for slot in _sweep_slots(r)
        length(s.workers) >= length(todo) + length(taken) && break
        slot in have && continue
        try
            push!(s.workers, _task_worker(r, s.parent, slot))
        catch e
            ReportEngine._rlog("region[$(r.name)]: could not start a task worker — " * first(sprint(showerror, e), 200))
            break
        end
    end
    for w in s.workers
        isempty(w.chunk) || continue
        isempty(todo) && break
        c = popfirst!(todo)
        res = try
            ReportEngine._tool(w.kernel, "__slate_run_chunk",
                               Dict{String,Any}("root" => store, "chunk" => c, "owner" => w.kernel.label); timeout = 30.0)
        catch e
            Dict{String,Any}("error" => first(sprint(showerror, e), 200))
        end
        if haskey(res, "error")
            ReportEngine._rlog("region[$(r.name)]: $(w.kernel.label) did not take chunk $c: $(res["error"])")
        else
            w.chunk = c
            ReportEngine._rlog("region[$(r.name)]: chunk $c → $(w.kernel.label)")
        end
    end
    return nothing
end
