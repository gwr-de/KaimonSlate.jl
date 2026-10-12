# Ordering and backpressure on the page WebSocket. `_ws_send!` writes straight to the socket when
# nothing is queued and falls back to the queue otherwise, so frames can take either of two routes —
# and the thing that must never happen is one overtaking the other.
using ReTest
using KaimonSlate.NotebookServer: _wsconn, _ws_send!, _ws_drain!, _raw_send
import KaimonSlate.NotebookServer as NS

# A stand-in for the socket. These are concurrency invariants, and a real WebSocket is the one thing
# a test of them does not need — what matters is the ORDER bytes are handed over in, and by whom.
mutable struct MockWS
    got::Vector{Any}
    lk::ReentrantLock
    delay::Float64      # pretend the client is slow, to force the queue into play
end
MockWS(delay = 0.0) = MockWS(Any[], ReentrantLock(), delay)
NS._raw_send(ws::MockWS, frame) = lock(ws.lk) do
    ws.delay > 0 && sleep(ws.delay)
    push!(ws.got, frame)
end

# Emitting is single-producer in the hot path: the poller task publishes one frame after another, so
# what has to hold is that they ARRIVE in that order — whichever route each one took.
@testset "frames keep their order across both routes" begin
    for delay in (0.0, 0.002)          # fast client → fast path; slow client → queue
        ws = MockWS(delay)
        c = _wsconn(1024); c.ws = ws
        drain = @async try; _ws_drain!(c); catch; end
        for i in 1:200; _ws_send!(c, "f$i"); end
        # Let the backlog finish, then stop the drain the way a closing socket would.
        t0 = time()
        while length(ws.got) < 200 && time() - t0 < 10; sleep(0.01); end
        close(c.out); sleep(0.05)

        @test length(ws.got) == 200            # nothing lost between the two routes
        @test ws.got == ["f$i" for i in 1:200] # …and nothing overtook anything
    end
end

# A backlog costs ONE wake, not one per frame: whatever is queued when the drain task gets the lock
# goes out in a single turn. This is the cheap half of the win — the expensive half is which pool the
# task wakes on, which a unit test cannot see.
@testset "a backlog drains in one turn" begin
    ws = MockWS()
    c = _wsconn(1024); c.ws = ws
    lock(c.wlock)                        # hold the socket so a backlog builds up behind it
    drain = @async try; _ws_drain!(c); catch; end
    for i in 1:100; _ws_send!(c, "f$i"); end
    yield()
    @test Base.n_avail(c.out) == 100     # nothing delivered yet — the drain is blocked on the lock
    unlock(c.wlock)
    t0 = time(); while length(ws.got) < 100 && time() - t0 < 5; sleep(0.01); end
    close(c.out); sleep(0.05)
    @test ws.got == ["f$i" for i in 1:100]
end

# The queue is what protects the emitter from a slow client, and it must still do that: emitting has
# to stay non-blocking even when the socket is crawling.
@testset "a slow client never blocks the emitter" begin
    ws = MockWS(0.05)                    # 50ms per frame — far slower than we emit
    c = _wsconn(1024); c.ws = ws
    drain = @async try; _ws_drain!(c); catch; end
    t0 = time()
    for i in 1:50; _ws_send!(c, "f$i"); end
    elapsed = time() - t0
    close(c.out)
    @test elapsed < 0.5                  # 50 frames would be 2.5s if we waited on the socket
end

# Large frames are bounded by their bytes, not only their count: a page that falls behind a stream of
# big binary frames holds at most `maxbytes` here, and is told about the rest.
@testset "a slow page's queue is bounded in bytes" begin
    ws = MockWS(0.05)
    c = _wsconn(1024; maxbytes = 10 * 2^20); c.ws = ws
    drain = @async try; _ws_drain!(c); catch; end
    peak = 0
    for i in 1:40
        _ws_send!(c, zeros(UInt8, 4 * 2^20))
        peak = max(peak, c.queued[])
    end
    @test peak <= 10 * 2^20
    @test c.dropped[] > 0
    t0 = time(); while Base.n_avail(c.out) > 0 && time() - t0 < 5; sleep(0.01); end
    sleep(0.1)
    @test c.queued[] == 0                 # every byte counted in is counted out
    _ws_send!(c, zeros(UInt8, 20 * 2^20))  # larger than the bound, into an empty queue: still sent
    t0 = time(); while !any(x -> x isa Vector{UInt8} && length(x) == 20 * 2^20, ws.got) && time() - t0 < 5; sleep(0.01); end
    close(c.out); sleep(0.05)
    @test any(x -> x isa Vector{UInt8} && length(x) == 20 * 2^20, ws.got)
end

# A frame's "sent" callback runs once per page: after the write, at once when the frame is dropped, and
# when the page goes away with the frame still queued. It is told which: only a write grants the emit
# channel its credit back. The emit channel's buffers and credit depend on it.
@testset "a frame's sent callback runs once, whatever becomes of it" begin
    # Sent: after the stand-in socket has it.
    ws = MockWS(); c = _wsconn(1024); c.ws = ws
    drain = @async try; _ws_drain!(c); catch; end
    got = Bool[]; f = zeros(UInt8, 100)
    _ws_send!(c, f; on_sent = w -> push!(got, w))
    t0 = time(); while isempty(got) && time() - t0 < 5; sleep(0.01); end
    @test got == [true]
    @test any(x -> x === f, ws.got)
    @test isempty(c.onsent)
    close(c.out)

    # Dropped: the queue is over its byte bound, so the callback runs at once.
    c2 = _wsconn(1024; maxbytes = 100)
    lock(c2.wlock)                                   # nothing drains
    _ws_send!(c2, zeros(UInt8, 80))
    got2 = Bool[]
    _ws_send!(c2, zeros(UInt8, 80); on_sent = w -> push!(got2, w))
    @test got2 == [false] && isempty(c2.onsent)

    # The page went away with frames still queued: their callbacks run when it is torn down.
    got3 = Bool[]
    c3 = _wsconn(1024)
    for _ in 1:3; _ws_send!(c3, zeros(UInt8, 10); on_sent = w -> push!(got3, w)); end
    close(c3.out); NS._ws_release_pending!(c3)
    @test got3 == [false, false, false]
    _ws_send!(c3, zeros(UInt8, 10); on_sent = w -> push!(got3, w))   # closed: at once
    @test got3 == fill(false, 4)
end

# A page that stops reading keeps a full queue forever. Once it has gone `WS_STALL_S` without a write,
# the next drop closes it, and the frame being written when that happens keeps its callback until the
# write ends.
mutable struct StuckWS
    aborted::Bool
    gate::Channel{Nothing}
end
NS._raw_send(ws::StuckWS, frame) = take!(ws.gate)
NS._raw_abort(ws::StuckWS) = (ws.aborted = true; close(ws.gate))
@testset "a page that stops reading is closed" begin
    ws = StuckWS(false, Channel{Nothing}(0))
    c = _wsconn(1024; maxbytes = 100); c.ws = ws
    drain = @async try; _ws_drain!(c); catch; end
    got = Bool[]
    _ws_send!(c, zeros(UInt8, 80); on_sent = w -> push!(got, w))   # taken by the drain, write blocks
    t0 = time(); while Base.n_avail(c.out) > 0 && time() - t0 < 5; sleep(0.01); end
    _ws_send!(c, zeros(UInt8, 80))                                  # queued behind it
    _ws_send!(c, zeros(UInt8, 80))                                  # dropped, but not stalled long
    @test !ws.aborted && isopen(c.out)
    NS._ws_release_pending!(c)
    @test isempty(got)                                              # the blocked write still owns its buffer
    c.wrote_at[] = time() - NS.WS_STALL_S[] - 1
    _ws_send!(c, zeros(UInt8, 80))                                  # dropped, and stalled: closed
    @test ws.aborted && !isopen(c.out)
    t0 = time(); while isempty(got) && time() - t0 < 5; sleep(0.01); end
    @test got == [false]                                            # the aborted write ends unwritten
end

# Overflow keeps its old contract: drop, count, and say so — never silently coalesce.
@testset "overflow still reports itself" begin
    ws = MockWS(0.01)
    c = _wsconn(4); c.ws = ws            # tiny queue, deliberately overrun
    drain = @async try; _ws_drain!(c); catch; end
    for i in 1:200; _ws_send!(c, "f$i"); end
    @test c.dropped[] > 0                # the burst outran the client, as intended
    # The marker rides the NEXT successful enqueue — a drop is reported to the client that missed it,
    # not counted in silence. So it appears once there is room again, not while the queue is jammed.
    t0 = time(); while Base.n_avail(c.out) > 0 && time() - t0 < 5; sleep(0.01); end
    _ws_send!(c, "after")
    t0 = time()
    while !any(x -> x isa String && occursin("\"t\":\"dropped\"", x), ws.got) && time() - t0 < 5; sleep(0.01); end
    close(c.out); sleep(0.05)
    marker = findfirst(x -> x isa String && occursin("\"t\":\"dropped\"", x), ws.got)
    @test marker !== nothing
    @test ws.got[marker + 1] == "after"  # …immediately before the frame that carried it
end
