# Kaimon extension TUI panel for KaimonSlate (rendered in the Extensions tab — press `u`).
# Shows where the hub is serving: its port is only a preference, so it may have moved to a free one.
# Lets you set the worker Julia-thread count, which governs parallel cell execution:
#   • 1 compute thread  → independent cells overlap only at yield points (I/O / sleep / async).
#   • N compute threads → independent cells run on real OS threads (multi-core CPU parallelism).
# The setting is persisted by the extension (KaimonSlate.set_worker_threads!) and applied by
# respawning the running notebooks' workers. Built entirely from Tachikoma 2.3 widgets.
#
# Panel protocol (see Kaimon docs): init(ctx)→state, update!(state,ctx), view(state,area,buf),
# handle_key!(state,evt)::Bool, cleanup!(state,ctx). `ctx.eval(code)` runs code in the extension
# process and returns (; stdout, stderr, value_repr). It reports a failure in `stderr` rather than
# throwing — the extension still starting, or restarted under a session this panel no longer names.

using Tachikoma

# Offered thread specs: "<compute>,<interactive>". TWO interactive threads are reserved so the gate stays
# responsive (heartbeats/cancels + reactive handling) even while a data transfer or compute batch runs;
# vary the compute count. `auto` lets Julia pick (≈ all cores).
const _OPTIONS = ["1,2", "2,2", "4,2", "8,2", "auto,2"]

const _REFRESH_S = 2.0   # how often the hub URL and thread setting are re-read while the panel is open

mutable struct PanelState
    list::Tachikoma.SelectableList
    ctx::Any
    current::String     # spec currently applied in the extension ("" → the adaptive default, min(cores,8),2)
    ncpu::Int
    url::String         # where the extension's hub is serving
    reach::String       # why the extension could not be asked ("" when it answered)
    status::String
    last::Float64       # when the last refresh started
    busy::Bool          # a refresh is in flight
end

# Kaimon up to 2.10.4 looks up the panel's own extension with extension sessions filtered out, so
# `ctx.eval` answers "extension session not found" to every call. Until a release passes
# `include_extensions = true` there, make the same lookup with the filter off. Remove once the
# minimum Kaimon has the fix.
function _eval(ctx, code::AbstractString)
    r = ctx.eval(String(code))
    occursin("session not found", string(get(r, :stderr, ""))) || return r
    K = parentmodule(typeof(ctx))
    conn = try
        K.get_connection_by_key(ctx.conn_mgr, ctx.session_key; include_extensions = true)
    catch
        nothing
    end
    conn === nothing && return r
    return K.eval_remote(conn, String(code))
end

# `value_repr` of `code` evaluated in the extension, or an error carrying what went wrong.
function _ask(ctx, code::AbstractString)::String
    r = _eval(ctx, code)
    err = strip(String(something(get(r, :stderr, ""), "")))
    ex = get(r, :exception, nothing)
    (ex === nothing || ex == "") || error(string(ex))
    startswith(err, "Error") && error(err)
    return String(something(get(r, :value_repr, ""), ""))
end

# Read the hub URL, thread setting and CPU count in one round trip. `nothing` fields mean unknown.
function _read(ctx)
    v = _ask(ctx, "(KaimonSlate._base(), KaimonSlate.worker_threads(), Sys.CPU_THREADS)")
    m = match(r"^\(\s*\"([^\"]*)\"\s*,\s*\"([^\"]*)\"\s*,\s*(\d+)\s*\)$", strip(v))
    m === nothing && error("unexpected reply: $(first(v, 120))")
    return (url = String(m.captures[1]), cur = String(m.captures[2]), ncpu = parse(Int, m.captures[3]))
end

_items(eff) = [Tachikoma.ListItem(o == eff ? "$o   ● current" : o, Tachikoma.tstyle(:text))
               for o in _OPTIONS]

_effective(cur) = isempty(cur) ? "1,1" : cur

function _apply_read!(state, r)
    state.url = r.url; state.ncpu = r.ncpu; state.reach = ""
    eff = _effective(r.cur)
    if eff != state.current
        state.current = eff
        state.list.items = _items(eff)
    end
    return state
end

# Off the render loop: `ctx.eval` blocks until the extension answers.
function _refresh!(state)
    state.busy && return
    state.busy = true; state.last = time()
    errormonitor(@async try
        _apply_read!(state, _read(state.ctx))
    catch e
        state.reach = sprint(showerror, e)
    finally
        state.busy = false
    end)
    return
end

function init(ctx)
    list = Tachikoma.SelectableList(_items("1,1"); selected = 1, focused = true)
    state = PanelState(list, ctx, "1,1", 0, "", "", "↑/↓ choose · Enter apply (respawns workers) · Esc close",
                       0.0, false)
    try
        _apply_read!(state, _read(ctx))
        state.list.selected = something(findfirst(==(state.current), _OPTIONS), 1)
    catch e
        state.reach = sprint(showerror, e)
    end
    return state
end

update!(state, ctx) = (time() - state.last > _REFRESH_S && _refresh!(state); nothing)

function view(state, area::Tachikoma.Rect, buf)
    hh = min(7, area.height)
    hdr = Tachikoma.Rect(area.x, area.y, area.width, hh)
    hub = !isempty(state.reach) ? "extension not reachable — $(first(state.reach, 80)) (reopen the panel after a restart)" :
          isempty(state.url) ? "starting…" : state.url
    p = Tachikoma.Paragraph(Tachikoma.Span[
        Tachikoma.Span("Hub  ", Tachikoma.tstyle(:title, bold = true)),
        Tachikoma.Span(hub * "\n\n", Tachikoma.tstyle(isempty(state.reach) ? :accent : :warning, bold = true)),
        Tachikoma.Span("Worker Julia threads\n", Tachikoma.tstyle(:title, bold = true)),
        Tachikoma.Span("applied: ", Tachikoma.tstyle(:text_dim)),
        Tachikoma.Span(state.current, Tachikoma.tstyle(:accent, bold = true)),
        Tachikoma.Span("   ·   CPU threads: $(state.ncpu)\n", Tachikoma.tstyle(:text_dim)),
        Tachikoma.Span(state.status, Tachikoma.tstyle(:text_dim)),
    ]; wrap = Tachikoma.word_wrap)
    Tachikoma.render(p, hdr, buf)
    if area.height > hh
        lrect = Tachikoma.Rect(area.x, area.y + hh, area.width, area.height - hh)
        state.list.block = Tachikoma.Block(title = "thread spec  (compute,interactive)",
                                           border_style = Tachikoma.tstyle(:border))
        Tachikoma.render(state.list, lrect, buf)
    end
end

function handle_key!(state, evt::Tachikoma.KeyEvent)::Bool
    if evt.key == :enter
        spec = _OPTIONS[state.list.selected]
        try
            _ask(state.ctx, "KaimonSlate.set_worker_threads!(\"$spec\")")
            # Read it back: the panel reports what the extension now holds, not what was asked for.
            r = _read(state.ctx)
            _apply_read!(state, r)
            state.status = _effective(r.cur) == spec ?
                "✓ applied and saved $spec — workers respawned (re-run cells to use the new threads)" :
                "⚠ asked for $spec but the extension reports $(_effective(r.cur))"
        catch e
            state.status = "⚠ not applied: $(first(sprint(showerror, e), 160))"
        end
        return true
    end
    return Tachikoma.handle_key!(state.list, evt)   # ↑/↓/Home/End/PgUp/PgDn navigation
end

cleanup!(state, ctx) = nothing
