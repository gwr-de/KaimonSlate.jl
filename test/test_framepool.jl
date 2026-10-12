# Reused buffers for large binary stream frames: `encode_binary_frame!` must produce exactly the frame
# the browser and the hub already read, and a buffer reused from frame to frame must stop allocating.
using ReTest
import SlateExtensionsBase as SEB

include(joinpath(@__DIR__, "..", "src", "framepool.jl"))

# The frame layout as `encode_binary_frame` wrote it before it encoded into a caller's buffer: the
# reference the new encoder is held to, byte for byte.
function reference_frame(channel, x::SEB.SlateBinary{T,N}) where {T,N}
    ch = codeunits(String(channel)); mb = codeunits(sprint(SEB._write_json, x.meta))
    io = IOBuffer()
    write(io, 0x01)
    write(io, UInt16(length(ch))); write(io, ch)
    write(io, UInt16(length(mb))); write(io, mb)
    write(io, SEB._bin_dtype(T))
    write(io, UInt8(N)); for d in size(x.data); write(io, UInt32(d)); end
    write(io, reinterpret(UInt8, vec(x.data)))
    return take!(io)
end

@testset "frame pool" begin
    @testset "encode_binary_frame! writes the same bytes as before, for every dtype and rank" begin
        for row in SEB.DTYPES, dims in ((5,), (3, 4), (2, 3, 4), (2, 2, 3, 2))
            data = row.T === Bool ? rand(Bool, dims...) : rand(row.T, dims...)
            x = SEB.SlateBinary(data; kind = "t", i = 3)
            want = reference_frame("chan", x)
            @test SEB.encode_binary_frame("chan", x) == want
            @test SEB.encode_binary_frame!(fill(0xaa, 7), "chan", x) == want   # a used buffer, resized
        end
    end

    @testset "a reused buffer is not reallocated" begin
        x = SEB.SlateBinary(rand(Float32, 64, 64, 64); kind = "fieldSub")
        buf = SEB.encode_binary_frame!(UInt8[], "c", x)
        SEB.encode_binary_frame!(buf, "c", x)
        @test (@allocated SEB.encode_binary_frame!(buf, "c", x)) < 4096   # the small header only
        @test (@allocated SEB.encode_binary_frame("c", x)) >= sizeof(x.data)
    end

    @testset "the pool reuses what is given back and keeps at most cap idle buffers" begin
        p = FramePool(2)
        a = take_buffer!(p); b = take_buffer!(p); c = take_buffer!(p)
        @test a !== b && b !== c                 # none idle: each take is a new buffer
        give_buffer!(p, a); give_buffer!(p, b); give_buffer!(p, c)
        @test idle_buffers(p) == 2               # the third is dropped
        @test take_buffer!(p) === b && take_buffer!(p) === a
        @test idle_buffers(p) == 0
    end
end
