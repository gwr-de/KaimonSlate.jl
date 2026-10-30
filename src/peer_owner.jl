# Which local account opened a loopback TCP connection.
#
# Loopback is no access boundary on a shared host: every account on a login node can connect to its
# 127.0.0.1. The kernel records who owns each end of a connection, and Linux reports it through
# `sock_diag`: one netlink request names a TCP socket by its exact address pair and the reply carries
# the uid that created it. This is what `ss` uses. `/proc/net/tcp` holds the same answer, but reading
# it costs tens of milliseconds on a busy node, which is too slow to pay per request.
#
# A connection that arrives through an SSH forward (`ssh -L`, VS Code's port forwarding) is opened by
# the forwarding process, which runs as the account that logged in, so it reads as that user.
#
# Pure and dependency-free, so its test can include it on its own (test/test_peer_owner.jl).

module PeerOwner

const AF_INET = 2
const AF_INET6 = 10
const AF_NETLINK = 16
const SOCK_DGRAM = 2
const SOCK_CLOEXEC = 0o2000000
const NETLINK_SOCK_DIAG = 4
const SOCK_DIAG_BY_FAMILY = 20
const NLMSG_ERROR = 2
const NLM_F_REQUEST = 1
const IPPROTO_TCP = 6
const SOL_SOCKET = 1
const SO_RCVTIMEO = 20

const NLMSG_HDRLEN = 16
const DIAG_REQ_LEN = NLMSG_HDRLEN + 56     # nlmsghdr + inet_diag_req_v2
const DIAG_MSG_LEN = 72                    # inet_diag_msg
const UID_OFFSET = 64                      # of `idiag_uid` within inet_diag_msg

"Is `ip` (4 or 16 bytes, network order) a loopback address — 127/8, `::1`, or `::ffff:127.x.y.z`?"
function is_loopback(ip)::Bool
    b = collect(UInt8, ip)
    length(b) == 4 && return b[1] == 0x7f
    length(b) == 16 || return false
    all(iszero, b[1:15]) && b[16] == 0x01 && return true
    return all(iszero, b[1:10]) && b[11] == 0xff && b[12] == 0xff && b[13] == 0x7f
end

"The IPv4-mapped IPv6 form of a 4-byte address."
v4mapped(ip) = UInt8[zeros(UInt8, 10); 0xff; 0xff; collect(UInt8, ip)]

_pad16(ip) = (b = collect(UInt8, ip); UInt8[b; zeros(UInt8, 16 - length(b))])

"""
    diag_request(family, src, sport, dst, dport) -> Vector{UInt8}

A `SOCK_DIAG_BY_FAMILY` request for the one TCP socket whose local end is `src:sport` and remote end
`dst:dport`. Addresses are network-order bytes; netlink's own header fields are host order.
"""
function diag_request(family::Integer, src, sport::Integer, dst, dport::Integer; seq::Integer = 1)
    io = IOBuffer()
    # nlmsghdr
    write(io, UInt32(DIAG_REQ_LEN), UInt16(SOCK_DIAG_BY_FAMILY), UInt16(NLM_F_REQUEST),
          UInt32(seq), UInt32(0))
    # inet_diag_req_v2: family, protocol, extensions, pad, states (all)
    write(io, UInt8(family), UInt8(IPPROTO_TCP), UInt8(0), UInt8(0), typemax(UInt32))
    # inet_diag_sockid: ports in network order, then source and destination, any interface, and
    # INET_DIAG_NOCOOKIE so the kernel matches on the addresses alone
    write(io, hton(UInt16(sport)), hton(UInt16(dport)), _pad16(src), _pad16(dst),
          UInt32(0), typemax(UInt32), typemax(UInt32))
    return take!(io)
end

"The uid in a `sock_diag` reply, or `nothing` when the kernel found no such socket (or answered oddly)."
function reply_uid(resp::AbstractVector{UInt8})::Union{UInt32,Nothing}
    length(resp) >= NLMSG_HDRLEN || return nothing
    typ = reinterpret(UInt16, resp[5:6])[1]
    typ == SOCK_DIAG_BY_FAMILY || return nothing          # NLMSG_ERROR: ENOENT for an unknown socket
    length(resp) >= NLMSG_HDRLEN + DIAG_MSG_LEN || return nothing
    o = NLMSG_HDRLEN + UID_OFFSET
    return reinterpret(UInt32, resp[o+1:o+4])[1]
end

# One request/reply over a fresh netlink socket. A socket per lookup keeps this free of shared state
# across threads, and costs microseconds. The receive timeout bounds a kernel that never answers.
function _query(req::Vector{UInt8})::Vector{UInt8}
    fd = ccall(:socket, Cint, (Cint, Cint, Cint), AF_NETLINK, SOCK_DGRAM | SOCK_CLOEXEC, NETLINK_SOCK_DIAG)
    fd < 0 && error("sock_diag: socket() failed: $(Libc.strerror(Libc.errno()))")
    try
        tv = Int64[1, 0]                                   # struct timeval: 1 s
        ccall(:setsockopt, Cint, (Cint, Cint, Cint, Ptr{Int64}, Cuint), fd, SOL_SOCKET, SO_RCVTIMEO, tv, 16)
        kernel = UInt8[reinterpret(UInt8, [UInt16(AF_NETLINK)]); zeros(UInt8, 10)]   # sockaddr_nl, pid 0
        n = ccall(:sendto, Cssize_t, (Cint, Ptr{UInt8}, Csize_t, Cint, Ptr{UInt8}, Cuint),
                  fd, req, length(req), 0, kernel, length(kernel))
        n < 0 && error("sock_diag: sendto() failed: $(Libc.strerror(Libc.errno()))")
        buf = Vector{UInt8}(undef, 8192)
        n = ccall(:recv, Cssize_t, (Cint, Ptr{UInt8}, Csize_t, Cint), fd, buf, length(buf), 0)
        n < 0 && error("sock_diag: recv() failed: $(Libc.strerror(Libc.errno()))")
        return resize!(buf, n)
    finally
        ccall(:close, Cint, (Cint,), fd)
    end
end

"""
    owner_uid(peer_ip, peer_port, local_ip, local_port) -> Union{UInt32,Nothing}

The uid that owns the far end of a loopback TCP connection this process accepted: the socket whose
local end is `peer_ip:peer_port` and whose remote end is `local_ip:local_port`. `nothing` when the
kernel has no such socket. Linux only; throws if `sock_diag` itself fails.

An IPv4 peer is looked up twice when needed: a dual-stack client socket connecting to an IPv4 address
is an AF_INET6 socket holding the IPv4-mapped form.
"""
function owner_uid(peer_ip, peer_port::Integer, local_ip, local_port::Integer)
    p = collect(UInt8, peer_ip); l = collect(UInt8, local_ip)
    if length(p) == 4 && length(l) == 4
        uid = reply_uid(_query(diag_request(AF_INET, p, peer_port, l, local_port)))
        uid === nothing || return uid
        return reply_uid(_query(diag_request(AF_INET6, v4mapped(p), peer_port, v4mapped(l), local_port)))
    end
    length(p) == 4 && (p = v4mapped(p))
    length(l) == 4 && (l = v4mapped(l))
    return reply_uid(_query(diag_request(AF_INET6, p, peer_port, l, local_port)))
end

"This process's real uid."
my_uid() = ccall(:getuid, Cuint, ())

end # module PeerOwner
