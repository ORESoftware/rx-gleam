# Hardening and semantic guarantees

`rx_gleam` keeps the core deliberately small, synchronous, and runtime-agnostic. This document records the invariants that changes must preserve.

## Observable guarantees

- `Observable` is cold: each terminal subscription opens a fresh `Source`.
- Operators do not start BEAM processes, JavaScript tasks, schedulers, or hidden workers.
- `take(count)` with `count <= 0` does not open upstream.
- `skip(count)` with `count <= 0` returns the original observable unchanged.
- `Stop` ends a subscription without pulling another upstream continuation and without invoking `on_complete`.
- A `Failed(error)` terminal step invokes `on_error`, never `on_complete`.
- `map_error` changes only the error channel.
- Built-in terminal consumers call the source cleanup callback once on normal completion, failure, or observer-requested stop.
- A panic raised by user code is outside the library's normal cleanup guarantee; `rx_gleam` does not claim exception/finalizer semantics it cannot enforce portably across Erlang and JavaScript.

## Eager guarantees

- `Eager` is a finite materialized sequence. Its operators execute immediately.
- Errors short-circuit subsequent eager operators.
- `map`, `filter`, `take`, `scan`, `flat_map`, `append`, and `fold` preserve input order.
- Internal large-list transformations use tail-recursive accumulator loops so the same APIs are practical on the Erlang and JavaScript targets.
- Conversion from lazy to eager is terminal and subscribes immediately; conversion from eager to lazy creates a cold observable over the materialized result.

## Error model

Errors are typed values, not exceptions. Sources terminate with `Failed(error)`, and eager sequences store `Result(List(value), error)`. The core intentionally does not catch panics from user callbacks.

## Concurrency model

The core owns no concurrency. A custom source can receive actor messages, perform FFI, block on I/O, delegate to a child process, or remain entirely synchronous. Those choices live outside the observable abstraction.

## Verification matrix

Every change must pass:

1. formatting checks;
2. `gleam check` on Erlang;
3. tests on Erlang;
4. `gleam check` on JavaScript;
5. tests on JavaScript;
6. large-sequence tests that exercise eager list transformations and long lazy runs;
7. an external consumer test from the `oresoftware-test` organization using `rx_gleam` as a SHA-pinned Git dependency.

The external consumer is important: it detects packaging, public API, dependency-resolution, and real-application integration failures that in-repository tests can miss.
