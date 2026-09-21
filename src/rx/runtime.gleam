import gleam/erlang/process
import gleam/otp/actor
import gleam/result

pub opaque type Runtime {
  Runtime(process.Subject(Message))
}

type Message {
  Run(fn() -> Nil)
  Stop
}

pub fn start() -> Result(Runtime, actor.StartError) {
  actor.new(Nil)
  |> actor.on_message(handle_message)
  |> actor.start
  |> result.map(fn(started) {
    let actor.Started(pid: _, data: subject) = started
    Runtime(subject)
  })
}

pub fn dispatch(runtime: Runtime, work: fn() -> Nil) -> Nil {
  let Runtime(subject) = runtime
  process.send(subject, Run(work))
}

pub fn stop(runtime: Runtime) -> Nil {
  let Runtime(subject) = runtime
  process.send(subject, Stop)
}

fn handle_message(_state: Nil, message: Message) -> actor.Next(Nil, Message) {
  case message {
    Run(work) -> {
      work()
      actor.continue(Nil)
    }
    Stop -> actor.stop()
  }
}
