# Buffers for binary stream frames on the emit channel, reused from frame to frame. The worker encodes a
# frame into a buffer taken here and gives it back once `send` has copied the bytes into libzmq; the hub
# receives into one and gives it back once every page has sent it. Without this, every frame of a stream
# of large frames was a new allocation of the frame's size, and the collector ran full collections to
# keep up.
#
# At most `cap` idle buffers are kept, so a stream that has stopped holds no more than that. A buffer
# taken while none is idle is a new one, and one given back to a full pool is dropped: either way the
# stream goes on, and only the reuse is lost.

mutable struct FramePool
    free::Vector{Vector{UInt8}}
    cap::Int
    lock::ReentrantLock
end
FramePool(cap::Integer) = FramePool(Vector{UInt8}[], Int(cap), ReentrantLock())

"An idle buffer from `p`, or a new empty one when none is idle."
take_buffer!(p::FramePool) = lock(() -> isempty(p.free) ? UInt8[] : pop!(p.free), p.lock)

"Give `buf` back to `p`; dropped when `p` already holds `cap` idle buffers."
give_buffer!(p::FramePool, buf::Vector{UInt8}) =
    lock(() -> (length(p.free) < p.cap && push!(p.free, buf); nothing), p.lock)

"How many idle buffers `p` holds."
idle_buffers(p::FramePool) = lock(() -> length(p.free), p.lock)
