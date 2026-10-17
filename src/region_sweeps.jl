# ── Region sweeps: the hub runs their chunks on a region's node ─────────────────────────────────
# A job cell with `region=<name>` and no `cluster=` queues its chunks in the region's store
# (`Sweep.RegionLauncher`). Here the hub takes them: on the node the region holds it starts a task
# worker, started in the notebook's environment like the region's own worker, so it is synced and
# Revise'd the same way and sees the whole node. How the body uses the node (threads, one GPU or all of
# them) is the body's own business. The worker is handed the chunks queued and runs them one after
# another, claiming each in the store, so loading and compilation are paid once.
#
# The store is the only channel between the notebook and this driver. The card submits by writing a
# want, reads progress from the store and the claims, and cancels by removing the want; the driver
# reads the queue, starts and stops the worker, and removes a want once each of its chunks has returned.
# A hub that restarts finds the queue where it was, and a worker that dies leaves a claim that goes
# stale, so its chunk is taken again.

mutable struct _TaskWorker
    kernel::ReportEngine.GateKernel
    chunk::String                   # the chunk it is running
    busy::Bool                      # working through the chunks it was handed
end

mutable struct _RegionSweeper
    region::String
    parent::String                  # the notebook environment its worker starts in
    host::String                    # the node it runs on; a new node means a new worker
    job::String
    worker::Union{Nothing,_TaskWorker}
    idle_since::Float64             # when the queue last had nothing for this worker (0 = busy)
end

const _SWEEPERS = Dict{Tuple{String,String},_RegionSweeper}()   # (region, parent) → its worker
const _SWEEPERS_LOCK = ReentrantLock()

"How long a task worker stays up with nothing queued for it, in seconds."
const _SWEEP_IDLE_STOP = 120.0

# The task worker: the region's target for this environment, as the region's own worker has it.
function _task_worker(r, parent::AbstractString)
    t = ReportEngine._region_target(r; origin_env = parent)
    label = basename(rstrip(String(parent), '/')) * "#" * r.name * "#task"
    k = ReportEngine.GateKernel(t.project; parent = String(parent), target = t,
                                threads = ReportEngine.region_threads(r), label = label)
    k.conn, k.tunnel = ReportEngine.spawn_and_connect_remote!(k, t, String(parent))
    ReportEngine._rlog("region[$(r.name)]: task worker $label up on $(t.ssh_host)")
    w = _TaskWorker(k, "", false)
    # Reattached to a worker a previous hub started: it may be part way through its chunks already.
    st = try; ReportEngine._tool(k, "__slate_chunk_status", Dict{String,Any}(); timeout = 15.0); catch; nothing; end
    (st !== nothing && get(st, "state", "") == "running") && (w.chunk = String(st["chunk"]); w.busy = true)
    return w
end

_running_stage(s::_RegionSweeper) = "task worker running on $(s.host)"

_stop_task_worker!(w::_TaskWorker) =
    (try; ReportEngine.shutdown!(w.kernel; kill_remote = true, wait = false); catch; end; nothing)

# Where a task worker that stopped answering is stuck, written into its own log before it is stopped:
# each thread's kernel wait state (`/proc/<pid>/task/*/wchan`, readable without attaching), and a native
# stack of every thread from `eu-stack` or `gdb` where the node lets one attach. Best effort: a node
# that cannot be reached, or forbids attaching, leaves what it could.
function _record_stuck_worker(w::_TaskWorker)
    k = w.kernel
    t = k.target
    (t isa ReportEngine.RemoteTarget && k.port != 0) || return nothing
    log = "$(ReportEngine._REMOTE_WORKER)/worker-$(k.port).log"
    script = """
    for p in \$(pgrep -u "\$USER" -f '[w]orker-$(k.port)\\.jl'); do
      {
        echo "=== worker pid \$p, not answering the hub, at \$(date '+%F %T') ==="
        for d in /proc/\$p/task/*; do
          echo "thread \$(basename \$d) \$(cut -d' ' -f3 \$d/stat 2>/dev/null) \$(cat \$d/wchan 2>/dev/null) \$(cat \$d/comm 2>/dev/null)"
        done
        if command -v eu-stack >/dev/null 2>&1; then timeout 60 eu-stack -p \$p
        elif command -v gdb >/dev/null 2>&1; then timeout 60 gdb -p \$p -batch -ex 'thread apply all bt'
        else echo '(no eu-stack or gdb on this node)'; fi
        echo "=== end of worker pid \$p ==="
      } >> $(ReportEngine.Sweep.shq(log)) 2>&1
    done
    """
    try
        ReportEngine._run_on(t.ssh_host, script; timeout = 120.0)
        ReportEngine._rlog("region: recorded where task worker $(k.label) was stuck, in $(t.ssh_host):$log")
    catch e
        ReportEngine._rlog("region: could not record where task worker $(k.label) was stuck — " * first(sprint(showerror, e), 120))
    end
    return nothing
end

function _stop_sweeper!(s::_RegionSweeper, why::AbstractString)
    s.worker === nothing && return nothing
    ReportEngine._rlog("region[$(s.region)]: stopping its task worker — $why")
    _stop_task_worker!(s.worker)
    s.worker = nothing
    return nothing
end
_busy(s::_RegionSweeper) = s.worker !== nothing && s.worker.busy

"""
    _drive_region_sweeps!(h)

One pass over every region with a store for sweeps: read its queue, and start, feed or stop its task
worker to match. Runs on the supervisor's tick, one pass per region at a time (`_region_work!`).
"""
function _drive_region_sweeps!(h)
    now = time()
    # Without `_tested`: driving a region's queue never reads it, and computing it each tick is a
    # readiness check of every environment tested anywhere.
    for e in ReportEngine.clusters_resolved(; tested = false)
        String(get(e, "kind", "")) == "region" || continue
        name = String(get(e, "_region", ""))
        r = ReportEngine.region_get(name)
        r === nothing && continue
        # Every 10 s while the region has work queued or a task worker, every 30 s otherwise: each
        # pass is one command on the login host, and an idle store costs only that.
        active = lock(_SWEEPERS_LOCK) do
            name in _SWEEP_QUEUED || any(s -> s.region == name && s.worker !== nothing, values(_SWEEPERS))
        end
        every = now < _SWEEP_FAST_UNTIL[] ? 5.0 : active ? 10.0 : 30.0
        now - get(_SWEEP_POLLED, name, 0.0) < every && continue
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
# What the driver last wrote to each region's `queue/stage`, for the card to show (`_set_stage!`).
const _SWEEP_STAGE = Dict{String,String}()
# Until when every region is looked at each 5 s: set by a card that starts work (`_sweep_kick!`), long
# enough to cover the card's next poll, which is what writes the submission.
const _SWEEP_FAST_UNTIL = Ref(0.0)
_sweep_kick!() = (_SWEEP_FAST_UNTIL[] = time() + 60.0; nothing)
const _SWEEP_QUEUED = Set{String}()       # regions whose last pass found work queued, under `_SWEEPERS_LOCK`

# Say what the driver is doing for region `r`, in its queue, where the card's poll reads it. Written only
# when it changes; "" removes it.
function _set_stage!(r, store::AbstractString, runner, text::AbstractString)
    BL = ReportEngine.Sweep.BatchLauncher
    lock(_SWEEPERS_LOCK) do; get(_SWEEP_STAGE, r.name, nothing) == text; end && return nothing
    f = BL._shq(joinpath(store, "queue", "stage"))
    ok, _ = runner(r.host, isempty(text) ? "rm -f $f" :
                           "mkdir -p $(BL._shq(joinpath(store, "queue"))) && printf '%s' $(BL._shq(text)) > $f")
    ok && lock(_SWEEPERS_LOCK) do; _SWEEP_STAGE[r.name] = String(text); end
    return nothing
end

function _drive_region_sweep!(r, store::AbstractString)
    BL = ReportEngine.Sweep.BatchLauncher
    runner = (hh, sc) -> ReportEngine.Sweep.run_there(hh, sc)
    ReportEngine.Sweep.connected(r.host) || return nothing        # nobody signed in: nothing to read
    q = BL.read_queue(runner, r.host, store)
    lock(_SWEEPERS_LOCK) do
        isempty(q.wants) ? delete!(_SWEEP_QUEUED, r.name) : push!(_SWEEP_QUEUED, r.name)
    end
    wanted = Set{String}(c for w in values(q.wants) for c in w.chunks)
    mine = lock(_SWEEPERS_LOCK) do; [s for s in values(_SWEEPERS) if s.region == r.name]; end
    foreach(s -> _check_worker!(s, r, q, wanted), mine)
    byparent = Dict{String,Vector{String}}()    # environment → its wants
    for (n, w) in q.wants
        push!(get!(Vector{String}, byparent, w.parent), n)
    end
    # Workers with nothing queued for their environment stop after a while, not at once: the next
    # submission of an interactive session usually follows soon, and starting a worker is the slow part.
    for s in mine
        haskey(byparent, s.parent) && continue
        _busy(s) && continue
        s.idle_since == 0 && (s.idle_since = time())
        time() - s.idle_since > _SWEEP_IDLE_STOP && _stop_sweeper!(s, "nothing queued")
    end
    isempty(q.wants) && return _set_stage!(r, store, runner, "")
    _region_used!(r.name)                       # queued work keeps the node, as a running cell does
    host, job = ReportEngine.region_where(r)
    if r.scheduler !== :none && isempty(job)
        # The sweep asks for the node as a region cell does, unless the region's request was
        # withdrawn after the work was queued: a release is not undone by work that was waiting on it.
        queued_at = time() - minimum(w.age for w in values(q.wants))
        withdrawn = lock(_PLACING_LOCK) do; get(_WITHDRAWN_AT, r.name, 0.0); end
        if withdrawn > queued_at
            _set_stage!(r, store, runner, "not asking for a node: the region was released after this was queued")
        else
            _place_in_background!(r.name)
            _set_stage!(r, store, runner, "waiting for a node")
        end
        return nothing
    end
    for (parent, names) in byparent
        s = lock(_SWEEPERS_LOCK) do
            get!(_SWEEPERS, (r.name, parent)) do
                _RegionSweeper(r.name, parent, host, job, nothing, 0.0)
            end
        end
        (s.host, s.job) == (host, job) || (_stop_sweeper!(s, "the region has a new node"); s.host = host; s.job = job)
        _feed_sweeper!(s, r, store, q, names, runner)
    end
    return nothing
end

# What the worker is doing. One whose chunk is no longer wanted was cancelled with its want: it is
# stopped, and the units it landed stay landed. A unit can hold every thread of the worker, or a long
# collection can stop it, so a call it does not answer in time does not mean it is gone: the timer that
# refreshes its chunk's claim does, once the claim has gone stale.
function _check_worker!(s::_RegionSweeper, r, q, wanted::Set{String})
    w = s.worker
    w === nothing && return nothing
    st = try
        ReportEngine._tool(w.kernel, "__slate_chunk_status", Dict{String,Any}(); timeout = 15.0)
    catch e
        ReportEngine._rlog("region[$(r.name)]: task worker $(w.kernel.label) did not answer — " *
                           first(sprint(showerror, e), 120))
        nothing
    end
    if st === nothing
        BL = ReportEngine.Sweep.BatchLauncher
        if any(cl -> cl.owner == w.kernel.label && cl.age < BL.CLAIM_STALE_S, values(q.claims))
            w.busy = true
            return nothing
        end
        ReportEngine._rlog("region[$(r.name)]: task worker $(w.kernel.label) holds no live claim — stopping it")
        _record_stuck_worker(w)
        _stop_task_worker!(w); s.worker = nothing
        return nothing
    end
    state = String(get(st, "state", "idle"))
    w.chunk = String(get(st, "chunk", ""))
    w.busy = state == "running"
    if w.busy && !isempty(w.chunk) && !(w.chunk in wanted)
        ReportEngine._rlog("region[$(r.name)]: chunk $(w.chunk) is no longer wanted — stopping its worker")
        _stop_task_worker!(w); s.worker = nothing
        return nothing
    end
    state == "failed" && ReportEngine._rlog("region[$(r.name)]: $(w.kernel.label) stopped on chunk " *
                                            "$(w.chunk): " * String(get(st, "error", "")))
    return nothing
end

function _feed_sweeper!(s::_RegionSweeper, r, store, q, names, runner)
    BL = ReportEngine.Sweep.BatchLauncher
    # Chunks to give out: wanted, not returned, and not held by a live claim.
    free_claim(c) = (cl = get(q.claims, c, nothing); cl === nothing || cl.age >= BL.CLAIM_STALE_S)
    todo = unique!([c for n in sort!(collect(names)) for c in q.wants[n].chunks
                    if !(c in q.done) && free_claim(c)])
    # A want whose chunks have all returned is finished: its card reads the store for what landed.
    finished = [n for n in names if all(in(q.done), q.wants[n].chunks)]
    isempty(finished) || runner(r.host, "rm -f " * join((BL._shq(joinpath(store, "queue", f))
        for n in finished for f in [n * ".want"; [c * ".done" for c in q.wants[n].chunks]]), ' '))
    busy = _busy(s)
    s.idle_since = isempty(todo) && !busy ? (s.idle_since == 0 ? time() : s.idle_since) : 0.0
    busy && _set_stage!(r, store, runner, _running_stage(s))
    (isempty(todo) || busy) && return nothing
    if s.worker === nothing
        _set_stage!(r, store, runner, "starting a task worker on $(s.host)")
        s.worker = try
            _task_worker(r, s.parent)
        catch e
            ReportEngine._rlog("region[$(r.name)]: could not start a task worker — " * first(sprint(showerror, e), 200))
            return nothing
        end
        _busy(s) && return _set_stage!(r, store, runner, _running_stage(s))   # reattached mid-list
    end
    w = s.worker
    res = try
        ReportEngine._tool(w.kernel, "__slate_run_chunks",
                           Dict{String,Any}("root" => store, "chunks" => todo, "owner" => w.kernel.label); timeout = 30.0)
    catch e
        Dict{String,Any}("error" => first(sprint(showerror, e), 200))
    end
    if haskey(res, "error")
        ReportEngine._rlog("region[$(r.name)]: $(w.kernel.label) did not take its chunks: $(res["error"])")
    else
        w.busy = true
        ReportEngine._rlog("region[$(r.name)]: $(length(todo)) chunk(s) → $(w.kernel.label)")
        _set_stage!(r, store, runner, _running_stage(s))
    end
    return nothing
end
