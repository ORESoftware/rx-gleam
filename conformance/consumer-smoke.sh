#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
work=$(mktemp -d "${TMPDIR:-/tmp}/rx-gleam-consumer.XXXXXX")
trap 'rm -rf "$work"' EXIT HUP INT TERM

mkdir -p "$work/src"
cat > "$work/gleam.toml" <<EOF
name = "rx_gleam_consumer"
version = "0.1.0"
target = "erlang"

[dependencies]
rx_gleam = { path = "$root" }
EOF

cat > "$work/src/rx_gleam_consumer.gleam" <<'EOF'
import rx
import rx/eager
import rx/flow
import rx/future
import rx/runtime

pub fn smoke() -> Nil {
  let _ =
    eager.from_list([1, 2, 3])
    |> eager.map(fn(value) { value + 1 })
    |> eager.to_result

  let assert Ok(runtime_) = runtime.start()
  let source: rx.Observable(Int, String) =
    rx.of(1)
    |> flow.map_async(fn(value) { future.pure(value + 1) })
  let assert Ok(subscription) =
    rx.subscribe(
      source,
      runtime_,
      rx.observer(fn(_) { Nil }, fn(_) { Nil }, fn() { Nil }),
    )

  rx.unsubscribe(subscription)
  runtime.stop(runtime_)
}
EOF

cd "$work"
gleam deps download
gleam build --warnings-as-errors

echo "[rx-gleam consumer-smoke] PASS"
