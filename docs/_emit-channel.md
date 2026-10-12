# The emit channel: a worker's fourth socket, for `slate_emit`

Status: design, being implemented on `feat/mbse`. Untracked note, 2026-10-11.

## Why

`slate_emit` from a worker travels today on Kaimon's gate stream (the XPUB at the gate port + 1),
alongside `eval_complete`, `tool_complete`, debugger events, progress, live cell output, logs and
telemetry. In the worker all of it shares KaimonGate's one broadcaster queue; in the hub one poller
task drains the stream of every worker of every notebook. A stream of large binary frames (volume
playback, SlateBench) delays cell completions and reactivity for every open notebook, the stream has
no high-water mark at either end, and every change to the frame path is a change to KaimonGate, which
a worker's environment versions separately from the hub's Kaimon.

The blob channel (gate port + 2) is Slate's, but it is request/reply for content-addressed transfers
that land in the CAS on disk: the wrong shape for a live stream, and a long blob transfer must not
hold up a frame.

## What

A fourth channel per worker, owned by Slate, carrying **only `slate_emit` traffic**: JSON emits (any
Julia value) and binary emits (`SlateBinary`), in the order they were made. Everything else stays on
the gate stream: Kaimon's events and Slate's progress, cell output, logs, telemetry, binds, refresh,
prepare, revise and tool calls.

Target: SlateBench at least as fast as on the gate stream: over 1 GB/s, latency under 1 ms.

## Transport

Two unidirectional ZMQ sockets, each used by exactly one side and one task, so no socket is ever
shared between tasks or threads:

- **data**: worker `PUSH`, bound; hub `PULL`, connected. Each message is two parts: a kind byte, then
  the payload. Kind `B`: a binary frame in `encode_binary_frame`'s layout, forwarded to the page as is.
  Kind `J`: `channel`, `0x1f`, then the `Serialization` bytes of the value.
- **credit**: hub `PUSH`, connected; worker `PULL`, bound. Messages: `H<window>` (hello, the hub is
  attached and grants `window` bytes) and `C<bytes>` (a grant). Decimal ASCII.

Endpoints are chosen by the worker and reported through `__slate_ports` (`emit`, `emit_credit`), so
the hub connects to whatever it is told:

- Local workers, every platform: `tcp://127.0.0.1` on ports the system chooses at bind (`*`),
  reported through `__slate_ports` with a per-worker secret. On macOS, loopback TCP measured about
  five times faster than a Unix socket for 32 MB messages (4.2 against 0.87 GB/s between two
  processes) and half the latency for 1 MB ones (0.48 against 1.02 ms). Any local user can reach a
  loopback port, so both sockets are ZMQ PLAIN servers and a ZAP handler admits only the secret,
  checked once per connection; the traffic itself is not encrypted, and never leaves the machine.
- Remote workers: not in the first version (the hub only attaches to local workers). `tcp://` on
  dynamic ports reached through the blob channel's ssh forwarding is the follow-up, with the same
  protocol.

## Flow control

Credit-based, in bytes. The worker starts with none. The hub's hello grants a window (32 MB: enough to keep the path busy; at saturation every message waits behind it, so it is also the latency a flat-out producer sees).
Each emit spends its size. The hub grants a message's size back once every open page of the
notebook has sent it (or dropped it at the page queue's byte bound), so the window bounds what is
in flight between the worker's emit and the pages' sockets. A notebook with no page open is granted
back on receipt, as if sent.

When credit runs out, `slate_emit` waits for a grant: a producer runs at the rate of the slowest
page and nothing is dropped. After `EMIT_WAIT_S` (5 s) without enough credit it drops that message
and counts it, so a cell cannot hang on a page nobody is reading. A message larger than the whole
window goes once nothing else is in flight.

The worker uses the channel only while the hub is attached (a hello received). Before that, and on a
worker where it never starts, emits take the gate stream as today.

## Buffers

- Worker: a binary emit is encoded into a buffer from the worker's `FramePool`
  (`encode_binary_frame!`, falling back to `encode_binary_frame` on an older SlateExtensionsBase) and
  sent; `send` copies it into libzmq and returns, and the buffer goes straight back to the pool. No
  KaimonGate involvement.
- Hub: one receive task per attached worker, off the shared gate-stream poller. A message is received
  into a libzmq message and copied into a buffer from the hub's pool; the libzmq message is closed on
  the receiving task (no `ZMQ.Message` object, no finalizer). The buffer goes back to the pool when the
  last page has sent it, together with the credit.

## Hub routing

The receive task maps its worker to the notebook (`_GATE_SESSION`), then calls the same functions the
gate-stream poller does: `_do_emit_bin(rid, frame)` and `_do_emit(rid, channel, value)`. The page
queues gain a per-frame "sent" callback (called after the socket write, or at once when the frame is
dropped), which is how the last page releases the buffer and returns the credit.

## Tests

- Transport, in one process over `ipc://`: messages arrive in order, both kinds; credit bounds what is
  sent; an emit waits for a grant and goes when it comes; an emit with no grant drops after the wait;
  a message larger than the window goes when nothing is in flight; no attach means no channel use.
- Page queue: a frame queued to two pages is released once, after both have sent it; a frame dropped
  at the byte bound is released at once.
- SlateBench in a notebook on the mbse hub: throughput and latency against its numbers on the gate
  stream.
