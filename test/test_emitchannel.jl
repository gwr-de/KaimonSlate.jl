# The emit channel transport (src/emitchannel.jl): a worker's `slate_emit` traffic on a socket pair of
# its own, with credit-based flow control. Real ZMQ sockets on loopback, both ends in this process.
using ReTest
import ZMQ
include(joinpath(@__DIR__, "..", "src", "framepool.jl"))
include(joinpath(@__DIR__, "..", "src", "emitchannel.jl"))

const SECRET = "test-secret"
# Loopback ports left to the system; the server reports what it bound.
server() = emit_server!(ZMQ, "tcp://127.0.0.1:*", "tcp://127.0.0.1:*"; secret = SECRET)
client(s, f; kw...) = emit_client!(ZMQ, s.data_endpoint, s.credit_endpoint, f; secret = SECRET, kw...)

waitfor(f; timeout = 10) = (t0 = time(); while !f() && time() - t0 < timeout; sleep(0.01); end; f())

# A hub that records what arrives, and releases each message when the test says so (or at once).
mutable struct Recorder
    got::Vector{Tuple{UInt8,Vector{UInt8}}}
    releases::Vector{Any}
    hold::Bool
    lock::ReentrantLock
end
Recorder(; hold = false) = Recorder(Tuple{UInt8,Vector{UInt8}}[], Any[], hold, ReentrantLock())
handler(r::Recorder) = (kind, buf, release, grant) -> lock(r.lock) do
    push!(r.got, (kind, copy(buf)))
    r.hold ? push!(r.releases, release) : release()
end
received(r::Recorder) = lock(() -> length(r.got), r.lock)
release_all!(r::Recorder) = lock(() -> (foreach(f -> f(), r.releases); empty!(r.releases)), r.lock)

@testset "emit channel" begin
    @testset "a client without the secret cannot attach" begin
        s = server()
        r = Recorder()
        h = emit_client!(ZMQ, s.data_endpoint, s.credit_endpoint, handler(r); secret = "wrong")
        try
            sleep(1.0)
            @test !emit_attached(s)
            @test emit_send!(s, EMIT_KIND_JSON, UInt8[1]) === :detached
        finally
            emit_close!(h); emit_close!(s)
        end
    end

    @testset "the server reports the ports it bound" begin
        s = server()
        try
            @test occursin(r"^tcp://127\.0\.0\.1:\d+$", s.data_endpoint)
            @test occursin(r"^tcp://127\.0\.0\.1:\d+$", s.credit_endpoint)
            @test s.data_endpoint != s.credit_endpoint
        finally
            emit_close!(s)
        end
    end

    @testset "nothing uses the channel before a hub attaches" begin
        s = server()
        try
            @test !emit_attached(s)
            @test emit_send!(s, EMIT_KIND_JSON, UInt8[1, 2, 3]) === :detached
        finally
            emit_close!(s)
        end
    end

    @testset "messages of both kinds arrive in order, and their buffers are reused" begin
        s = server()
        r = Recorder()
        h = client(s, handler(r); window = 1 << 20)
        try
            @test waitfor(() -> emit_attached(s))
            msgs = [(isodd(i) ? EMIT_KIND_BINARY : EMIT_KIND_JSON, rand(UInt8, 1000 + i)) for i in 1:200]
            for (k, p) in msgs
                @test emit_send!(s, k, p) === :sent
            end
            @test waitfor(() -> received(r) == 200)
            @test r.got == msgs
            @test idle_buffers(h.pool) > 0       # released buffers went back to the hub's pool
        finally
            emit_close!(h); emit_close!(s)
        end
    end

    @testset "credit bounds what is in flight, and a grant lets the rest through" begin
        s = server()
        r = Recorder(hold = true)
        h = client(s, handler(r); window = 3000)
        try
            @test waitfor(() -> emit_attached(s))
            @test emit_send!(s, EMIT_KIND_BINARY, zeros(UInt8, 1000)) === :sent
            @test emit_send!(s, EMIT_KIND_BINARY, zeros(UInt8, 1000)) === :sent
            @test emit_send!(s, EMIT_KIND_BINARY, zeros(UInt8, 1000)) === :sent
            # The window is spent: the next send waits for a grant, which comes when the hub releases.
            t = Threads.@spawn emit_send!(s, EMIT_KIND_BINARY, zeros(UInt8, 1000); wait_s = 10)
            sleep(0.3)
            @test !istaskdone(t)
            @test waitfor(() -> received(r) == 3)
            release_all!(r)
            @test fetch(t) === :sent
            @test waitfor(() -> received(r) == 4)
        finally
            release_all!(r); emit_close!(h); emit_close!(s)
        end
    end

    @testset "a send with no grant in time detaches the channel, and it resumes on its own" begin
        old = EMIT_RESYNC_S[]; EMIT_RESYNC_S[] = 0.5
        s = server()
        r = Recorder(hold = true)
        h = client(s, handler(r); window = 1000)
        try
            @test waitfor(() -> emit_attached(s))
            @test emit_send!(s, EMIT_KIND_BINARY, zeros(UInt8, 1000)) === :sent
            # The hub holds what it was sent: no grant comes, so the next send detaches the channel
            # and the caller sends that message the other way.
            @test emit_send!(s, EMIT_KIND_BINARY, zeros(UInt8, 1000); wait_s = 0.2) === :detached
            @test s.stalls[] == 1 && !emit_attached(s)
            @test emit_send!(s, EMIT_KIND_BINARY, UInt8[1]) === :detached
            # The worker asks to resume, and the hub's fresh hello restores the whole window.
            @test waitfor(() -> emit_attached(s); timeout = 10)
            @test s.credit == 1000
            @test emit_send!(s, EMIT_KIND_BINARY, zeros(UInt8, 1000)) === :sent
        finally
            EMIT_RESYNC_S[] = old
            release_all!(r); emit_close!(h); emit_close!(s)
        end
    end

    @testset "a grant lets the worker go on while the buffer is still held" begin
        # The hub's way with several pages: credit back once the first has sent a frame, the buffer
        # only once the last has. A page that is slow to read holds buffers, not the worker.
        s = server()
        held = Any[]; lk = ReentrantLock(); n = Threads.Atomic{Int}(0)
        h = client(s, (kind, buf, release, grant) -> begin
                grant(); lock(() -> push!(held, release), lk); Threads.atomic_add!(n, 1)
            end; window = 3000)
        try
            @test waitfor(() -> emit_attached(s))
            for _ in 1:10
                @test emit_send!(s, EMIT_KIND_BINARY, zeros(UInt8, 1000); wait_s = 5) === :sent
            end
            @test waitfor(() -> n[] == 10)
            @test s.stalls[] == 0                 # ten window-sized sends' worth, nothing released yet
        finally
            lock(() -> foreach(f -> f(), held), lk)
            emit_close!(h); emit_close!(s)
        end
    end

    @testset "a message larger than the window goes once nothing is in flight" begin
        s = server()
        r = Recorder()
        h = client(s, handler(r); window = 1000)
        try
            @test waitfor(() -> emit_attached(s))
            big = rand(UInt8, 5000)
            @test emit_send!(s, EMIT_KIND_BINARY, big) === :sent
            @test waitfor(() -> received(r) == 1) && r.got[1][2] == big
            @test emit_send!(s, EMIT_KIND_BINARY, UInt8[7]; wait_s = 5) === :sent   # credit came back
        finally
            emit_close!(h); emit_close!(s)
        end
    end

    @testset "a hub that detaches tells the worker, which stops using the channel at once" begin
        s = server()
        r = Recorder()
        h = client(s, handler(r); window = 1 << 20)
        try
            @test waitfor(() -> emit_attached(s))
            emit_close!(h)
            @test waitfor(() -> !emit_attached(s); timeout = 3)   # no send needed to find out
            @test (@elapsed emit_send!(s, EMIT_KIND_JSON, UInt8[1])) < 0.5
        finally
            emit_close!(s)
        end
    end

    @testset "a hub that goes away detaches the channel instead of blocking it" begin
        s = server()
        r = Recorder()
        h = client(s, handler(r); window = 1 << 20)
        try
            @test waitfor(() -> emit_attached(s))
            @test emit_send!(s, EMIT_KIND_JSON, UInt8[1]) === :sent
            emit_close!(h)
            # The data socket has no peer now: a send times out and the channel detaches.
            res = emit_send!(s, EMIT_KIND_JSON, UInt8[2])
            res === :sent && (res = emit_send!(s, EMIT_KIND_JSON, UInt8[3]))
            @test waitfor(() -> !emit_attached(s) || res === :detached; timeout = 10)
            # A new hub attaches and the channel carries messages again.
            r2 = Recorder()
            h2 = client(s, handler(r2); window = 1 << 20)
            try
                @test waitfor(() -> emit_attached(s))
                @test emit_send!(s, EMIT_KIND_JSON, UInt8[9]) === :sent
                @test waitfor(() -> received(r2) >= 1) && last(r2.got)[2] == UInt8[9]
            finally
                emit_close!(h2)
            end
        finally
            emit_close!(s)
        end
    end
end
