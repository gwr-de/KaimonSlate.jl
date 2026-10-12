# ── The emit channel: a worker's `slate_emit` traffic on a socket pair of its own ──────────────────
#
# See docs/_emit-channel.md. The transport in isolation: ZMQ is passed in (`Z`), so this file runs in
# the worker (KaimonGate's ZMQ) and the hub (Kaimon's) alike, and is tested in one process over ipc.
#
# Two one-way sockets, each with a single user, so no socket is ever shared between tasks:
#   data:   worker PUSH (bound) → hub PULL (connected). Two parts: a kind byte, then the payload.
#           `B` = a binary frame in `encode_binary_frame`'s layout; `J` = channel, 0x1f, Serialization
#           bytes of the value.
#   credit: hub PUSH (connected) → worker PULL (bound). `H<n>` = attached, `n` bytes granted;
#           `C<n>` = `n` more bytes granted. Decimal ASCII.
#
# Flow control is credit in bytes. A send spends its size; the hub grants it back once the pages have
# taken it. Out of credit, a send waits up to `wait_s` for a grant.
#
# The channel heals itself. A send that gets no grant in time detaches the channel, and the caller sends
# that message, and the ones after it, the other way (the gate stream). While detached the worker asks
# the hub to resume (`R` on the data socket), at first after `EMIT_RESYNC_S` and, when the channel stalls
# again soon after resuming, at doubling intervals up to `EMIT_RESYNC_MAX_S`; the hub answers with a
# fresh hello, which restores the whole window. Grants are capped at the window, so credit for messages
# from before a reset cannot raise it further.
#
# Local workers use TCP on loopback: on macOS it moves large messages several times faster than a Unix
# socket. Any local user can reach a loopback port, so with a `secret` both sockets are PLAIN servers
# and a ZAP handler admits only a client presenting it; the hub learns it over the gate. PLAIN checks
# the secret once, at connect, and leaves the traffic itself untouched.
#
# Needs `framepool.jl` included first, in the same module.

import FileWatching

const EMIT_KIND_BINARY = 0x42   # 'B'
const EMIT_KIND_JSON = 0x4a     # 'J'
const EMIT_WINDOW_BYTES = Ref(32 * 1024 * 1024)   # what may be in flight; at saturation, the delay a message waits behind
const EMIT_WAIT_S = Ref(5.0)
const EMIT_RESYNC_S = Ref(5.0)
const EMIT_RESYNC_MAX_S = 60.0
const EMIT_KIND_RESYNC = 0x52   # 'R': worker → hub, "send a fresh hello"

# ── worker side ───────────────────────────────────────────────────────────────────────────────────

mutable struct EmitServer
    Z::Module
    ctx::Any
    data::Any                    # PUSH, bound; used only under `cond`
    credit_sock::Any             # PULL, bound; used only by the credit task
    data_endpoint::String
    credit_endpoint::String
    cond::Threads.Condition      # guards `data`, `credit`, `window`, `attached`
    credit::Int
    window::Int
    attached::Bool
    sent::Threads.Atomic{Int}
    stalls::Threads.Atomic{Int}  # sends that found no credit in time and detached the channel
    running::Threads.Atomic{Bool}
    task::Any
    zap::Any                     # the ZAP handler's task, when there is a secret
    hub_seen::Bool               # a hub has attached at some point, so a detached channel asks to resume
    attached_at::Float64
    resync_after::Float64        # the current interval between requests to resume
    next_resync::Float64
end

"""
    emit_server!(Z, data_endpoint, credit_endpoint; secret = "") -> EmitServer

Bind the worker's two emit sockets and start the task that takes the hub's credit. An endpoint may
leave the port to the system (`tcp://127.0.0.1:*`); `data_endpoint` and `credit_endpoint` on the
result are the bound addresses. With a `secret`, only a client presenting it can connect. The channel
is used only once the hub has attached (`emit_attached`).
"""
function emit_server!(Z::Module, data_endpoint::AbstractString, credit_endpoint::AbstractString;
                      secret::AbstractString = "")
    ctx = Z.Context()
    running = Threads.Atomic{Bool}(true)
    zap = isempty(secret) ? nothing : _emit_zap!(Z, ctx, String(secret), running)
    # A PUSH with no peer blocks its send, and a hub that has gone away leaves it with none: a send
    # checks for a peer first (`_writable_within`), and the channel counts as detached until the hub
    # attaches again.
    data = Z.Socket(ctx, Z.PUSH); data.linger = 0; data.sndhwm = 0
    cs = Z.Socket(ctx, Z.PULL); cs.linger = 0; cs.rcvtimeo = 500
    if !isempty(secret)
        for sock in (data, cs)
            _setopt_int(Z, sock, Z.lib.ZMQ_PLAIN_SERVER, 1)
            _setopt_str(Z, sock, Z.lib.ZMQ_ZAP_DOMAIN, _EMIT_ZAP_DOMAIN)
        end
    end
    Z.bind(data, String(data_endpoint))
    Z.bind(cs, String(credit_endpoint))
    s = EmitServer(Z, ctx, data, cs, _bound_endpoint(data), _bound_endpoint(cs),
                   Threads.Condition(), 0, 0, false, Threads.Atomic{Int}(0), Threads.Atomic{Int}(0),
                   running, nothing, zap, false, 0.0, EMIT_RESYNC_S[], 0.0)
    s.task = Threads.@spawn _emit_credit_loop(s)
    return s
end

_bound_endpoint(sock) = rstrip(String(sock.last_endpoint), '\0')

const _EMIT_ZAP_DOMAIN = "slate-emit"
const _EMIT_PLAIN_USER = "slate"

function _setopt_int(Z::Module, sock, opt::Integer, v::Integer)
    rc = Z.lib.zmq_setsockopt(sock, Cint(opt), Ref{Cint}(v), sizeof(Cint))
    rc == 0 || error("emit channel: setsockopt $opt failed")
    return nothing
end
function _setopt_str(Z::Module, sock, opt::Integer, s::AbstractString)
    b = codeunits(String(s))
    rc = GC.@preserve b Z.lib.zmq_setsockopt(sock, Cint(opt), pointer(b), length(b))
    rc == 0 || error("emit channel: setsockopt $opt failed")
    return nothing
end

# The ZAP handler for the channel's context: admits a PLAIN client whose password is `secret`, once per
# connection. Bound before either server socket, as ZAP requires.
function _emit_zap!(Z::Module, ctx, secret::String, running)
    zap = Z.Socket(ctx, Z.REP); zap.linger = 0; zap.rcvtimeo = 500
    Z.bind(zap, "inproc://zeromq.zap.01")
    return Threads.@spawn try
        while running[]
            req = try
                Z.recv_multipart(zap, Vector{UInt8})
            catch e
                e isa Z.TimeoutError && continue
                running[] || break
                continue
            end
            # version, request id, domain, address, identity, mechanism, then the credentials.
            ok = length(req) >= 8 && String(req[6]) == "PLAIN" && String(req[8]) == secret
            Z.send_multipart(zap, [req[1], req[2], ok ? "200" : "400", ok ? "OK" : "denied", "", ""])
        end
    finally
        try; close(zap); catch; end
    end
end

emit_attached(s::EmitServer) = lock(() -> s.attached, s.cond)

function _emit_credit_loop(s::EmitServer)
    Z = s.Z
    try
        while s.running[]
            msg = try
                Z.recv(s.credit_sock, String)
            catch e
                s.running[] || break
                e isa Z.TimeoutError || sleep(0.05)
                ""
            end
            if !isempty(msg)
                n = something(tryparse(Int, msg[2:end]), 0)
                lock(s.cond) do
                    if msg[1] == 'H'               # a hub (re)attached: whatever was in flight is gone
                        s.window = n; s.credit = n; s.attached = true
                        s.hub_seen = true; s.attached_at = time()
                    elseif msg[1] == 'C'
                        s.credit = min(s.window, s.credit + n)
                    elseif msg[1] == 'D'           # the hub is detaching: emits take the gate stream from now
                        s.attached = false; s.hub_seen = false
                    end
                    notify(s.cond)
                end
            end
            _maybe_resync!(s)
        end
    finally
        lock(s.cond) do; s.attached = false; notify(s.cond); end
    end
end

"""
    emit_send!(s, kind, payload; nbytes = length(payload), wait_s = EMIT_WAIT_S[]) -> Symbol

Send one message: `:sent`, or `:detached`, when no hub is attached or none granted credit within
`wait_s` (the channel then detaches and asks to resume later), so the caller sends it some other way. Returns once `send` has copied `payload` into libzmq, so the
caller may reuse it.
"""
function emit_send!(s::EmitServer, kind::UInt8, payload::Vector{UInt8};
                    nbytes::Integer = length(payload), wait_s::Real = EMIT_WAIT_S[])
    Z = s.Z
    lock(s.cond) do
        s.attached || return :detached
        deadline = time() + wait_s
        # A message larger than the whole window goes once nothing else is in flight.
        while s.credit < nbytes && !(s.credit >= s.window && nbytes > s.window)
            left = deadline - time()
            s.attached || return :detached
            if left <= 0
                _stalled!(s)
                return :detached
            end
            t = Timer(_ -> lock(() -> notify(s.cond), s.cond), left)
            try; wait(s.cond); finally; close(t); end
        end
        try
            _writable_within(Z, s.data, EMIT_PEER_WAIT_S) || error("no hub on the emit channel")
            Z.send(s.data, kind == EMIT_KIND_BINARY ? _KIND_B : _KIND_J; more = true)
            Z.send(s.data, payload)
        catch
            # No hub to take it (gone without detaching). The kind part may be queued alone, which
            # would misalign the next message, so the channel stays detached until a new hello.
            s.attached = false
            return :detached
        end
        s.credit -= nbytes
        Threads.atomic_add!(s.sent, 1)
        return :sent
    end
end
const _KIND_B = UInt8[EMIT_KIND_BINARY]
const _KIND_J = UInt8[EMIT_KIND_JSON]
const _KIND_R = UInt8[EMIT_KIND_RESYNC]

# No grant came in time: the hub is not taking what it was sent. Detach, so emits take the gate stream,
# and ask to resume after a while, waiting longer each time it stalls again soon after resuming. Called
# with `s.cond` held.
function _stalled!(s::EmitServer)
    Threads.atomic_add!(s.stalls, 1)
    s.attached = false
    s.resync_after = time() - s.attached_at < 30 ? min(EMIT_RESYNC_MAX_S, 2 * s.resync_after) : EMIT_RESYNC_S[]
    s.next_resync = time() + s.resync_after
    return nothing
end

# Detached from a hub that had attached: ask it, when due, to resume with a fresh hello.
function _maybe_resync!(s::EmitServer)
    lock(s.cond) do
        (s.attached || !s.hub_seen || time() < s.next_resync) && return nothing
        s.next_resync = time() + s.resync_after
        try
            _writable_within(s.Z, s.data, 0.2) || return nothing
            s.Z.send(s.data, _KIND_R; more = true)
            s.Z.send(s.data, UInt8[])
        catch
        end
    end
    return nothing
end
const EMIT_PEER_WAIT_S = 2.0

# Whether `sock` can take a message within `timeout` seconds. ZMQ.jl's `send` ignores `sndtimeo` and
# waits on the socket for as long as it takes, and a PUSH whose peer has gone is never writable. A
# PUSH with a peer and no high-water mark always is, so this costs one `ZMQ_EVENTS` read per send.
function _writable_within(Z::Module, sock, timeout::Real)
    (sock.events & Z.POLLOUT) != 0 && return true
    deadline = time() + timeout
    while (sock.events & Z.POLLOUT) == 0
        left = deadline - time()
        left <= 0 && return false
        try; FileWatching.poll_fd(Base.fd(sock), min(left, 0.1); readable = true); catch; end
    end
    return true
end

function emit_close!(s::EmitServer)
    s.running[] = false
    lock(s.cond) do; s.attached = false; notify(s.cond); end
    try; wait(s.task); catch; end
    lock(s.cond) do; try; close(s.data); catch; end; end
    try; close(s.credit_sock); catch; end
    s.zap === nothing || (try; wait(s.zap); catch; end)   # the context closes only once its sockets have
    try; close(s.ctx); catch; end
    return nothing
end

# ── hub side ──────────────────────────────────────────────────────────────────────────────────────

mutable struct EmitClient
    Z::Module
    ctx::Any
    data::Any                    # PULL, connected; used only by the receive task
    credit_sock::Any             # PUSH, connected; used only under `lock`
    lock::ReentrantLock
    pool::FramePool
    received::Threads.Atomic{Int}
    running::Threads.Atomic{Bool}
    task::Any
    # Released bytes not yet granted back. Grants go in chunks of `grant_every` bytes: one per message
    # would be a message back to the worker, and a wake of its credit task, for every frame received.
    ungranted::Threads.Atomic{Int}
    grant_every::Int
    window::Int
end

"""
    emit_client!(Z, data_endpoint, credit_endpoint, on_message; window, pool) -> EmitClient

Attach to a worker's emit channel and start receiving. `on_message(kind, buf, release, grant)` is called
on the receive task for each message, in order: `buf` is the payload in a buffer from `pool`. `grant()`
grants the message's size back to the worker, once the message has gone where it is going first.
`release()` must be called exactly once, when `buf` is no longer needed by anyone: it gives the buffer
back, and grants too if `grant` has not been called. Separating the two lets a consumer that is slow
to read hold its copy of the bytes without holding back the worker.
"""
function emit_client!(Z::Module, data_endpoint::AbstractString, credit_endpoint::AbstractString,
                      on_message; window::Integer = EMIT_WINDOW_BYTES[], pool::FramePool = FramePool(8),
                      secret::AbstractString = "")
    ctx = Z.Context()
    data = Z.Socket(ctx, Z.PULL); data.linger = 0; data.rcvhwm = 0; data.rcvtimeo = 500
    cs = Z.Socket(ctx, Z.PUSH); cs.linger = 0
    if !isempty(secret)
        for sock in (data, cs)
            _setopt_str(Z, sock, Z.lib.ZMQ_PLAIN_USERNAME, _EMIT_PLAIN_USER)
            _setopt_str(Z, sock, Z.lib.ZMQ_PLAIN_PASSWORD, secret)
        end
    end
    Z.connect(data, String(data_endpoint))
    Z.connect(cs, String(credit_endpoint))
    c = EmitClient(Z, ctx, data, cs, ReentrantLock(), pool, Threads.Atomic{Int}(0),
                   Threads.Atomic{Bool}(true), nothing, Threads.Atomic{Int}(0), max(1, Int(window) ÷ 16),
                   Int(window))
    lock(() -> Z.send(cs, "H" * string(Int(window))), c.lock)
    # On the interactive pool where there is one, as the hub's gate-stream poller is: this task wakes for
    # every message, and on the default pool each wake scans a run queue per thread.
    c.task = Threads.nthreads(:interactive) > 0 ? Threads.@spawn(:interactive, _emit_receive_loop(c, on_message)) :
                                                  Threads.@spawn(_emit_receive_loop(c, on_message))
    return c
end

function _emit_grant!(c::EmitClient, n::Integer)
    (n > 0 && c.running[]) || return nothing
    Threads.atomic_add!(c.ungranted, Int(n)) + n < c.grant_every && return nothing
    g = Threads.atomic_xchg!(c.ungranted, 0)
    g > 0 && lock(() -> (try; c.Z.send(c.credit_sock, "C" * string(g)); catch; end), c.lock)
    return nothing
end

# A part received into a buffer from the pool: received as a libzmq message, copied, and the message
# closed here, on this task. ZMQ.jl's `recv(sock, Vector{UInt8})` does the same into a new array.
function _emit_recv_into!(Z::Module, sock, pool::FramePool)
    if isdefined(Z, :msg_init) && isdefined(Z, :_recv!)
        zmsg = Z.msg_init()
        try
            Z._recv!(sock, zmsg)
            n = length(zmsg)
            buf = take_buffer!(pool)
            resize!(buf, n)
            n > 0 && GC.@preserve buf zmsg unsafe_copyto!(pointer(buf), Base.unsafe_convert(Ptr{UInt8}, zmsg), n)
            return buf
        finally
            close(zmsg)
        end
    end
    return Z.recv(sock, Vector{UInt8})
end

function _emit_receive_loop(c::EmitClient, on_message)
    Z = c.Z
    while c.running[]
        kind = try
            Z.recv(c.data, Vector{UInt8})
        catch e
            e isa Z.TimeoutError && continue
            c.running[] || break
            sleep(0.05); continue
        end
        buf = try
            _emit_recv_into!(Z, c.data, c.pool)
        catch
            c.running[] || break
            continue
        end
        # A worker asking to resume after a stall: a fresh hello restores its whole window.
        if !isempty(kind) && kind[1] == EMIT_KIND_RESYNC
            give_buffer!(c.pool, buf)
            Threads.atomic_xchg!(c.ungranted, 0)
            lock(() -> (try; Z.send(c.credit_sock, "H" * string(c.window)); catch; end), c.lock)
            continue
        end
        Threads.atomic_add!(c.received, 1)
        n = length(buf)
        granted = Threads.Atomic{Bool}(false)
        released = Threads.Atomic{Bool}(false)
        grant = () -> (Threads.atomic_xchg!(granted, true) || _emit_grant!(c, n); nothing)
        release = () -> begin
            Threads.atomic_xchg!(released, true) && return nothing
            grant()
            give_buffer!(c.pool, buf)
            return nothing
        end
        try
            # invokelatest: this task lives as long as the worker, in the world it started in.
            Base.invokelatest(on_message, isempty(kind) ? 0x00 : kind[1], buf, release, grant)
        catch e
            @debug "emit channel: message handler failed" exception = e
            release()
        end
    end
end

function emit_close!(c::EmitClient)
    # Tell the worker first, so its next emit takes the gate stream at once instead of waiting out a
    # send to a hub that is gone. The credit socket lingers long enough to deliver it.
    lock(c.lock) do
        try; c.credit_sock.linger = 1000; c.Z.send(c.credit_sock, "D0"); catch; end
    end
    c.running[] = false
    try; wait(c.task); catch; end
    try; close(c.data); catch; end
    lock(() -> (try; close(c.credit_sock); catch; end), c.lock)
    try; close(c.ctx); catch; end
    return nothing
end
