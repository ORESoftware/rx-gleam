import gleam/erlang/process
import gleeunit
import gleeunit/should
import rx
import rx/flow
import rx/future
import rx/runtime

pub type WorkerEvent {
  WorkerStarted(process.Subject(Result(Int, String)))
}

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn runtime_stop_cancels_active_future_once_test() {
  let workers = process.new_subject()
  let cancelled = process.new_subject()
  let outputs = process.new_subject()
  let assert Ok(runtime_) = runtime.start()
  let source: rx.Observable(Int, String) =
    rx.from_list([1])
    |> flow.merge_map(
      fn(value) {
        future.new(fn(resolve) {
          let pid =
            process.spawn_unlinked(fn() {
              let gate = process.new_subject()
              process.send(workers, WorkerStarted(gate))
              case process.receive(from: gate, within: 5000) {
                Ok(result) -> resolve(result)
                Error(Nil) -> resolve(Error("worker timeout"))
              }
            })

          fn() {
            process.send(cancelled, value)
            process.kill(pid)
          }
        })
      },
      1,
    )

  let assert Ok(_subscription) =
    rx.subscribe(
      source,
      runtime_,
      rx.observer(
        fn(value) { process.send(outputs, value) },
        fn(_) { Nil },
        fn() { Nil },
      ),
    )
  let assert Ok(WorkerStarted(gate)) =
    process.receive(from: workers, within: 1000)

  runtime.stop(runtime_)

  process.receive(from: cancelled, within: 1000) |> should.equal(Ok(1))
  process.receive(from: cancelled, within: 20) |> should.equal(Error(Nil))

  // The worker is physically cancelled. A late send to its old gate cannot
  // resurrect the stopped flow or produce downstream output.
  process.send(gate, Ok(10))
  process.receive(from: outputs, within: 20) |> should.equal(Error(Nil))
}

pub fn stopped_runtime_rejects_new_subscription_test() {
  let stopped = process.new_subject()
  let assert Ok(runtime_) = runtime.start()
  let live: rx.Observable(Int, String) =
    rx.create(fn(_) { fn() { process.send(stopped, Nil) } })
  let observer = rx.observer(fn(_) { Nil }, fn(_) { Nil }, fn() { Nil })
  let assert Ok(_subscription) = rx.subscribe(live, runtime_, observer)

  runtime.stop(runtime_)
  process.receive(from: stopped, within: 1000) |> should.equal(Ok(Nil))
  process.sleep(10)

  let later: rx.Observable(Int, String) = rx.of(1)
  rx.subscribe(later, runtime_, observer)
  |> should.equal(Error(runtime.RuntimeStopped))
}
