# ── The local blob cache ─────────────────────────────────────────────────────────────────────
# A blob in a store this machine cannot open is copied here a byte range at a time, into one sparse
# file per blob named by the blob's hash, with `<hash>.have` listing the byte ranges the file holds.
# A read fetches only the ranges it lacks: a selection that overlaps one already read costs only the
# difference, and every reader of the blob shares one copy — whichever host it came through, and
# whether it is a notebook, a worker or another program loading this module. The name is the
# content, so an entry can never be stale.
#
# Included into `Sweep` after `SshSource`, `_split_spans` and the fetch constants it uses.

import FileWatching

"Where the cache keeps its blobs: `<cache home>/remote-blobs`."
blob_cache_dir() = joinpath(SlateHome.cache_home(), "remote-blobs")
_cache_file(blob) = joinpath(blob_cache_dir(), String(blob))
_have_file(blob) = _cache_file(blob) * ".have"
_pin_file(blob) = _cache_file(blob) * ".pin"

"Fetches are rounded out to pages of this many bytes, so reads of neighbouring bytes coalesce."
const CACHE_PAGE = Ref(1024 * 1024)

# ── Byte spans ───────────────────────────────────────────────────────────────────────────────
# Half-open `(lo, hi)` byte intervals. A merged list is sorted and has no two that touch.

function _merge_spans(v)
    isempty(v) && return Tuple{Int,Int}[]
    s = sort!(Tuple{Int,Int}[(Int(a), Int(b)) for (a, b) in v if b > a]; by = first)
    out = Tuple{Int,Int}[]
    for (lo, hi) in s
        if !isempty(out) && lo <= out[end][2]
            out[end] = (out[end][1], max(out[end][2], hi))
        else
            push!(out, (lo, hi))
        end
    end
    return out
end

# The parts of `want` that `have` does not cover. Both merged.
function _subtract(want, have)
    out = Tuple{Int,Int}[]
    j = 1
    for (lo, hi) in want
        while j <= length(have) && have[j][2] <= lo; j += 1; end
        cur, k = lo, j
        while cur < hi
            if k > length(have) || have[k][1] >= hi
                push!(out, (cur, hi)); break
            end
            have[k][1] > cur && push!(out, (cur, have[k][1]))
            cur = max(cur, have[k][2]); k += 1
        end
    end
    return out
end

_span_bytes(v) = sum(s -> s[2] - s[1], v; init = 0)

# `spans` widened to whole cache pages and clipped to the blob's `limit`, so a stepped selection's
# thousands of short runs become a few contiguous reads.
function _page_spans(spans, limit::Integer)
    pg = CACHE_PAGE[]
    return _merge_spans([(lo - lo % pg, min(Int(limit), cld(hi, pg) * pg)) for (lo, hi) in spans])
end

# ── What the cache holds ─────────────────────────────────────────────────────────────────────

# In-process serialisation of `.have` updates; the pidfile serialises them across processes.
const _HAVE_LOCK = ReentrantLock()

_with_blob_lock(f, blob) = lock(_HAVE_LOCK) do
    FileWatching.Pidfile.mkpidlock(f, _have_file(blob) * ".lock"; stale_age = 60)
end

function _read_have(blob)
    f = _have_file(blob)
    out = Tuple{Int,Int}[]
    isfile(f) || return out
    for l in eachline(f)
        a = split(l)
        length(a) == 2 || continue
        lo, hi = tryparse(Int, a[1]), tryparse(Int, a[2])
        (lo === nothing || hi === nothing) || push!(out, (lo, hi))
    end
    return _merge_spans(out)
end

"The byte ranges of `blob` present in the cache (none when its file is gone or shorter than claimed)."
function cached_spans(blob::AbstractString)
    have = _read_have(blob)
    isempty(have) && return have
    p = _cache_file(blob)
    (isfile(p) && filesize(p) >= have[end][2]) || return Tuple{Int,Int}[]
    return have
end

# Replace the recorded spans of `blob`. The caller holds the blob's lock.
function _write_have!(blob, have)
    f = _have_file(blob)
    tmp = string(f, ".", getpid(), ".", string(rand(UInt32); base = 16))
    open(tmp, "w") do io
        for (lo, hi) in have; println(io, lo, ' ', hi); end
    end
    mv(tmp, f; force = true)
end

function _add_have!(blob, spans)
    isempty(spans) && return
    _with_blob_lock(() -> _write_have!(blob, _merge_spans(vcat(_read_have(blob), spans))), blob)
    return
end

# ── Keeping blob files sparse ────────────────────────────────────────────────────────────────
# A blob file holds only the ranges fetched; the rest must take no disk. A filesystem may or may not
# leave a hole when a write lands past the end of a file, so the gap is deallocated explicitly:
# F_PUNCHHOLE on macOS, fallocate(PUNCH_HOLE) on Linux. Elsewhere, or where the filesystem refuses,
# the file is merely larger on disk than it needs to be.

const _HOLE_ALIGN = 4096

# Deallocate [lo, hi) of the open file `io`, rounded inward to whole filesystem blocks.
function _punch_hole!(io::IOStream, lo::Integer, hi::Integer)
    a = cld(lo, _HOLE_ALIGN) * _HOLE_ALIGN
    b = fld(hi, _HOLE_ALIGN) * _HOLE_ALIGN
    b > a || return false
    fd = Base.Libc.fd(io)
    if Sys.isapple()
        # struct fpunchhole { unsigned int fp_flags; unsigned int reserved; off_t fp_offset; off_t fp_length; }
        arg = Ref((Cuint(0), Cuint(0), Int64(a), Int64(b - a)))
        return ccall(:fcntl, Cint, (Cint, Cint, Ptr{Cvoid}...), fd, 99, arg) == 0   # F_PUNCHHOLE
    elseif Sys.islinux()
        return ccall(:fallocate, Cint, (Cint, Cint, Int64, Int64), fd, 0x03, a, b - a) == 0  # PUNCH_HOLE | KEEP_SIZE
    end
    return false
end

"""
    compact_blob!(blob) -> Int

Give back to the filesystem every part of `blob`'s cache file outside the ranges it holds, returning
the bytes the file takes on disk afterwards.
"""
function compact_blob!(blob::AbstractString)
    path = _cache_file(blob)
    isfile(path) || return 0
    _with_blob_lock(blob) do
        gaps = _subtract([(0, filesize(path))], _merge_spans(_read_have(blob)))
        open(path, "r+") do io
            for (lo, hi) in gaps
                _punch_hole!(io, lo, hi)
            end
        end
    end
    return _on_disk(path)
end

"""
    evict!(blob, spans) -> Int

Remove the byte ranges `spans` (`(lo, hi)` half-open) of `blob` from the cache: they are dropped from
its record and their disk given back. Returns the bytes the file takes on disk afterwards. Evicting
everything removes the file.
"""
function evict!(blob::AbstractString, spans)
    path = _cache_file(blob)
    isfile(path) || return 0
    drop = _merge_spans(spans)
    _with_blob_lock(blob) do
        have = _subtract(_merge_spans(_read_have(blob)), drop)
        if isempty(have)
            rm(_have_file(blob); force = true)
            rm(path; force = true)
            return
        end
        _write_have!(blob, have)
        open(path, "r+") do io
            for (lo, hi) in _subtract([(0, filesize(path))], have)
                _punch_hole!(io, lo, hi)
            end
        end
    end
    return isfile(path) ? _on_disk(path) : 0
end

# Copy a landed piece — the bytes of `group` ((offset, length) pairs) back to back — into the blob's
# file at their offsets. A write past the end leaves the gap before it as a hole.
function _scatter!(blob, piece, group)
    path = _cache_file(blob)
    buf = Vector{UInt8}(undef, 4 << 20)
    # The bytes and the record of them change together, under the blob's lock: compaction and
    # eviction deallocate what the record does not list, so they must never see one without the other.
    _with_blob_lock(blob) do
        touch(path)                   # creates without truncating, and marks the blob used
        eof0 = filesize(path)
        open(path, "r+") do out
            open(piece, "r") do inp
                for (off, len) in group
                    seek(out, off)
                    left = len
                    while left > 0
                        n = readbytes!(inp, buf, min(left, length(buf)))
                        n == 0 && error("piece $(basename(piece)) is shorter than its ranges")
                        write(out, view(buf, 1:n))
                        left -= n
                    end
                end
            end
            start = minimum(first, group)
            start > eof0 && _punch_hole!(out, eof0, start)
        end
        _write_have!(blob, _merge_spans(vcat(_read_have(blob), [(o, o + l) for (o, l) in group])))
    end
end

# ── Fetching ─────────────────────────────────────────────────────────────────────────────────
# The chunks being fetched in this process, per blob. A fetch claims a chunk only as it starts it, so a
# reader wanting bytes another fetch has planned but not reached fetches them itself, and the other
# skips them when it gets there. A reader whose bytes are in a chunk already in flight waits for that
# chunk alone.
struct _Inflight
    lo::Int
    hi::Int
    owner::UInt64
end
const _INFLIGHT = Dict{String,Vector{_Inflight}}()
const _INFLIGHT_LOCK = ReentrantLock()

# Claim the spans of one chunk for `owner`, unless all of it is already here or any of it is in flight.
function _claim!(blob::String, spans, owner::UInt64)
    lock(_INFLIGHT_LOCK) do
        need = _subtract(_merge_spans(spans), cached_spans(blob))
        isempty(need) && return false
        fl = get!(() -> _Inflight[], _INFLIGHT, blob)
        any(x -> any(s -> s[1] < x.hi && x.lo < s[2], need), fl) && return false
        for (lo, hi) in need; push!(fl, _Inflight(lo, hi, owner)); end
        return true
    end
end

function _release!(blob::String, owner::UInt64)
    lock(_INFLIGHT_LOCK) do
        fl = get(_INFLIGHT, blob, nothing)
        fl === nothing && return
        filter!(x -> x.owner != owner, fl)
        isempty(fl) && delete!(_INFLIGHT, blob)
    end
end

_in_flight(blob::String, need) = lock(_INFLIGHT_LOCK) do
    fl = get(_INFLIGHT, blob, _Inflight[])
    any(x -> any(s -> s[1] < x.hi && x.lo < s[2], need), fl)
end

"Bytes fetched since the cache was last trimmed. A trim is due past an eighth of the cap."
const _BLOB_FETCHED = Ref{Int}(0)

"""
    cache_fetch!(src::SshSource, blob, spans; limit, label = "", hooks = READ_HOOKS[]) -> path

Make the byte ranges `spans` (`(offset, length)` pairs) of `blob` present in the cache, fetching only
what it lacks, and return the cache file's path. `limit` is the blob's size, which the page rounding
must not pass. `hooks` is `(; progress(frac, msg, done), checkpoint())`: `checkpoint` throwing an
`InterruptException` stops the fetch, or the wait for bytes another fetch has in flight, and what had
already landed is kept.
"""
function cache_fetch!(src::SshSource, blob::AbstractString, spans;
                      limit::Integer, label::AbstractString = "", hooks = READ_HOOKS[])
    blob = String(blob)
    mkpath(blob_cache_dir())
    want = _merge_spans([(o, o + l) for (o, l) in spans])
    total = _span_bytes(want)
    for _ in 1:4
        need = _subtract(want, cached_spans(blob))
        isempty(need) && return _cache_file(blob)
        _fetch_spans!(src, blob, _subtract(_page_spans(need, limit), cached_spans(blob)); label, hooks)
        # What another fetch had in flight is waited for here, a chunk at a time.
        while true
            need = _subtract(want, cached_spans(blob))
            (isempty(need) || !_in_flight(blob, need)) && break
            hooks === nothing || hooks.checkpoint()
            hooks === nothing || hooks.progress(1 - _span_bytes(need) / total,
                                                "waiting for $(label): $(_bytes(_span_bytes(need))) in flight", false)
            sleep(0.1)
        end
    end
    missing_ = _span_bytes(_subtract(want, cached_spans(blob)))
    missing_ == 0 && return _cache_file(blob)
    error("reading $(label) from $(src.host): $(_bytes(missing_)) of the selection could not be fetched")
end

# Fetch the merged byte spans `spans` of `blob` into the cache. Large reads are cut into chunks taken by
# several connections; each chunk is claimed as it starts, skipped when it is already here or another
# fetch has it, and written into the blob's file as soon as it lands, so a stopped or failed read keeps
# what it got and other readers see it at once.
function _fetch_spans!(src::SshSource, blob::String, spans::Vector{Tuple{Int,Int}};
                       label::AbstractString = "", hooks = READ_HOOKS[])
    isempty(spans) && return
    total = _span_bytes(spans)
    nchunks = total >= STREAM_MIN_BYTES[] ? max(FETCH_STREAMS[], cld(total, CHUNK_BYTES[])) : 1
    groups = _split_spans([(lo, hi - lo) for (lo, hi) in spans], nchunks)
    dir = blob_cache_dir()
    tag = string(getpid(), ".", string(rand(UInt32); base = 16))
    pieces = [joinpath(dir, string(blob, ".piece.", tag, ".", i)) for i in eachindex(groups)]
    parts = [pc * ".part" for pc in pieces]
    script = ranges_command(src.root, blob)
    input(g) = Vector{UInt8}(join((string(o, ' ', l, '\n') for (o, l) in g)))
    owners = [rand(UInt64) for _ in groups]
    span_of(g) = [(o, o + l) for (o, l) in g]
    committed = falses(length(groups))
    kept = Threads.Atomic{Int}(0)
    clock = ReentrantLock()
    claim(i) = _claim!(blob, span_of(groups[i]), owners[i])
    function commit(i)
        lock(clock) do
            committed[i] && return
            pc = pieces[i]
            if isfile(pc) && filesize(pc) == sum(last, groups[i]; init = 0)
                _scatter!(blob, pc, groups[i])
                committed[i] = true
                Threads.atomic_add!(kept, Int(filesize(pc)))
            end
            rm(pc; force = true)
        end
        _release!(blob, owners[i])
    end
    landed() = kept[] + sum((isfile(pt) ? Int(filesize(pt)) : isfile(pc) ? Int(filesize(pc)) : 0
                             for (i, (pt, pc)) in enumerate(zip(parts, pieces)) if !committed[i]); init = 0)
    t0 = time()
    outcome = "failed"
    got = Ref{Any}(nothing)                 # the results, for the cleanup below to read
    thrown = Ref("")                        # …or what the fetch threw instead
    results = try
        got[] = _await_fetch(landed, () -> foreach(pt -> (try; cancel_fetch!(pt); catch; end), parts),
                     total, label; hooks) do
            if total < BULK_BYTES[]        # small: one read on the host's session, no new connection
                claim(1) || return [(true, 0, "")]
                [fetch_to(src.host, script, pieces[1]; input = input(groups[1]), part = parts[1],
                          name = label, total)]
            else
                fetch_many(src.host, script, [(input(g), pieces[i], parts[i]) for (i, g) in enumerate(groups)];
                           streams = length(groups) > 1 ? FETCH_STREAMS[] : 1, name = label, total,
                           before_job = claim, on_job = commit)
            end
        end
    catch e
        e isa InterruptException ? (outcome = "stopped") : (thrown[] = first(sprint(showerror, e), 300))
        rethrow()
    finally
        # What landed is kept whether the rest succeeded, failed or was stopped.
        for i in eachindex(groups)
            try; commit(i); catch; end
            rm(parts[i]; force = true); rm(pieces[i]; force = true)
        end
        # Done when every chunk landed, here or by a reader that had claimed it; a chunk skipped for
        # another reader counts as done.
        fails = got[] === nothing ? String[] : String[r[3] for r in got[] if !r[1]]
        got[] !== nothing && isempty(fails) && (outcome = "done")
        why = outcome != "failed" ? "" : isempty(fails) ? thrown[] : fails[1]
        kept[] > 0 && _log_transfer(t0, time() - t0, src.host, blob, kept[], label, outcome, why)
    end
    _BLOB_FETCHED[] += kept[]
    if _BLOB_FETCHED[] > blob_cap() ÷ 8
        _BLOB_FETCHED[] = 0
        try; trim_blob_cache!(); catch; end   # best-effort: a full cache is not a failed read
    end
    ok = all(first, results)
    ok || error("reading $(_bytes(total)) of $(label) from $(src.host) failed: " *
                something(findfirst(r -> !r[1], results) |> (i -> i === nothing ? nothing : results[i][3]), "failed"))
    return
end

# ── The transfer log ─────────────────────────────────────────────────────────────────────────
# One line per fetch that landed bytes, kept beside the blobs so every program reading through this
# cache, notebooks included, writes and reads the same history: when, how long, from where, which
# blob, how many bytes, for what, whether it finished, and why not when it failed.

_transfer_log_file() = joinpath(blob_cache_dir(), "transfers.log")

function _log_transfer(t0, seconds, host, blob, bytes, label, outcome, why = "")
    clean(x) = replace(String(x), r"[\t\n]" => " ")
    line = join((round(t0; digits = 3), round(seconds; digits = 3), host, blob, bytes,
                 clean(label), outcome, first(clean(why), 300)), '\t') * '\n'
    try
        open(io -> write(io, line), _transfer_log_file(); append = true)
    catch
    end
    return
end

"""
    transfer_log(; blob = nothing, host = nothing, n = 1000) -> Vector{NamedTuple}

The fetches that landed bytes in this machine's blob cache, newest first:
`(; at, seconds, host, blob, bytes, label, outcome, why)`, where `outcome` is "done", "stopped" or
"failed" and `why` the failure's reason.
"""
function transfer_log(; blob = nothing, host = nothing, n::Integer = 1000)
    f = _transfer_log_file()
    T = NamedTuple{(:at, :seconds, :host, :blob, :bytes, :label, :outcome, :why),
                   Tuple{Float64,Float64,String,String,Int,String,String,String}}
    out = T[]
    isfile(f) || return out
    for l in Iterators.reverse(readlines(f))
        a = split(l, '\t')
        length(a) >= 7 || continue
        at, sec, b = tryparse(Float64, a[1]), tryparse(Float64, a[2]), tryparse(Int, a[5])
        (at === nothing || sec === nothing || b === nothing) && continue
        blob === nothing || a[4] == blob || continue
        host === nothing || a[3] == host || continue
        push!(out, T((at, sec, String(a[3]), String(a[4]), b, String(a[6]), String(a[7]),
                      length(a) >= 8 ? String(a[8]) : "")))
        length(out) >= n && break
    end
    return out
end

"""
    transfer_rate(host; n = 20) -> Union{Float64,Nothing}

Bytes per second recently achieved fetching from `host`: the bytes over the seconds of its last `n`
fetches of at least a megabyte, or `nothing` with no history to go on.
"""
function transfer_rate(host::AbstractString; n::Integer = 20)
    b = 0; s = 0.0; k = 0
    for e in transfer_log(; host = String(host), n = 10n)
        e.bytes >= 1 << 20 && e.seconds > 0 || continue
        b += e.bytes; s += e.seconds; k += 1
        k >= n && break
    end
    return s > 0 ? b / s : nothing
end

"The byte ranges of `blob` this process is fetching right now."
in_flight(blob::AbstractString) = lock(_INFLIGHT_LOCK) do
    _merge_spans([(x.lo, x.hi) for x in get(_INFLIGHT, String(blob), _Inflight[])])
end

# ── Keeping and evicting ─────────────────────────────────────────────────────────────────────

"Keep `blob` in the cache whatever its size and age; `trim_blob_cache!` never evicts a pinned blob."
pin!(blob::AbstractString) = (mkpath(blob_cache_dir()); touch(_pin_file(blob)); nothing)
unpin!(blob::AbstractString) = (rm(_pin_file(blob); force = true); nothing)
pinned(blob::AbstractString) = isfile(_pin_file(blob))

# Disk a file actually takes: a sparse blob's apparent size is the whole blob.
_on_disk(f) = (s = stat(f); s.blocks > 0 ? Int(s.blocks) * 512 : Int(s.size))

"""
    cache_entries() -> Vector{NamedTuple}

Every blob in the cache: `(; blob, bytes, on_disk, pinned, used)`, where `bytes` is what `.have`
records as present and `used` the last time it was read (unix time), most recent first.
"""
function cache_entries(dir::AbstractString = blob_cache_dir())
    out = NamedTuple{(:blob, :bytes, :on_disk, :pinned, :used),Tuple{String,Int,Int,Bool,Float64}}[]
    isdir(dir) || return out
    for name in readdir(dir)
        occursin('.', name) && continue
        f = joinpath(dir, name)
        isfile(f) || continue
        push!(out, (; blob = name, bytes = _span_bytes(cached_spans(name)), on_disk = _on_disk(f),
                      pinned = pinned(name), used = Float64(mtime(f))))
    end
    return sort!(out; by = e -> -e.used)
end

"Bytes the blob cache may hold, pinned blobs included. Free disk / 16, clamped, or `KAIMONSLATE_BLOB_CAP_GB`."
function _default_blob_cap()::Int
    free = try; Base.diskstat(dirname(blob_cache_dir())).available; catch; 0; end
    gb = 1024^3
    # A sixteenth rather than the memo store's quarter, and a lower ceiling: re-fetching costs
    # seconds on the connection that is already open, where recomputing a memo entry can cost hours.
    return free <= 0 ? 4gb : clamp(round(Int, free ÷ 16), gb, 8gb)
end
const _BLOB_CAP = Ref{Int}(0)
blob_cap() = (_BLOB_CAP[] > 0 ? _BLOB_CAP[] : (_BLOB_CAP[] =
    (v = tryparse(Float64, get(ENV, "KAIMONSLATE_BLOB_CAP_GB", "")); v !== nothing && v > 0 ?
        round(Int, v * 1024^3) : _default_blob_cap())))

"""
    trim_blob_cache!(dir = blob_cache_dir(); cap, grace = 300.0) -> Int

Evict least-recently-used blobs until the cache fits `cap`, returning the bytes freed. Pinned blobs
count toward the total but are never evicted. `grace` (seconds) covers two races: a piece another
process is still writing, and a blob this process has just returned to a caller about to map it.
"""
function trim_blob_cache!(dir::AbstractString = blob_cache_dir();
                          cap::Integer = blob_cap(), grace::Real = 300.0)
    isdir(dir) || return 0
    now = time()
    live = Tuple{String,Float64,Int}[]
    total = 0
    for name in readdir(dir)
        f = joinpath(dir, name)
        isfile(f) || continue
        mt = try; Float64(mtime(f)); catch; continue; end
        if occursin(".piece.", name) || (occursin(".have.", name) && !endswith(name, ".lock"))
            # Litter from a fetch or an update that died. Past the grace window nothing is writing it.
            now - mt > grace && try; rm(f; force = true); catch; end
            continue
        end
        occursin('.', name) && continue
        sz = try; _on_disk(f); catch; continue; end
        total += sz
        (now - mt > grace && !pinned(name)) && push!(live, (name, mt, sz))
    end
    total <= cap && return 0
    sort!(live; by = x -> x[2])                          # least recently used first
    freed = 0
    for (name, _, sz) in live
        total - freed <= cap && break
        try
            _with_blob_lock(name) do
                rm(_have_file(name); force = true)       # first, so no reader trusts a file being removed
                rm(joinpath(dir, name); force = true)
            end
            freed += sz
        catch
        end
    end
    return freed
end
