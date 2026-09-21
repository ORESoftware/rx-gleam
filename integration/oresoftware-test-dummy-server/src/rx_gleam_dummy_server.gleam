import gleam/bytes_tree
import gleam/erlang/process
import gleam/http/request.{type Request}
import gleam/http/response.{type Response}
import gleam/int
import gleam/list
import gleam/string
import mist.{type Connection, type ResponseData}
import rx
import rx/eager

const server_port = 8099

pub fn main() {
  let assert Ok(_) =
    handle_request
    |> mist.new
    |> mist.bind("127.0.0.1")
    |> mist.port(server_port)
    |> mist.start

  process.sleep_forever()
}

fn handle_request(request: Request(Connection)) -> Response(ResponseData) {
  case request.path_segments(request) {
    ["health"] -> text_response(200, "ok")
    ["lazy"] -> text_response(200, lazy_payload())
    ["eager"] -> text_response(200, eager_payload())
    ["bridge"] -> text_response(200, bridge_payload())
    ["error"] -> text_response(200, error_payload())
    _ -> text_response(404, "not-found")
  }
}

fn lazy_payload() -> String {
  let stream: rx.Observable(Int, String) =
    rx.from_list([1, 2, 3, 4, 5, 6])
    |> rx.map(fn(value) { value * 2 })
    |> rx.filter(fn(value) { value >= 6 })
    |> rx.take(3)

  case rx.to_list(stream) {
    Ok(values) -> render_ints(values)
    Error(error) -> "error:" <> error
  }
}

fn eager_payload() -> String {
  let sequence: eager.Eager(Int, String) =
    eager.from_list([1, 2, 3, 4])
    |> eager.map(fn(value) { value * 3 })
    |> eager.filter(fn(value) { value > 3 })
    |> eager.take(2)

  case eager.to_result(sequence) {
    Ok(values) -> render_ints(values)
    Error(error) -> "error:" <> error
  }
}

fn bridge_payload() -> String {
  let sequence: eager.Eager(Int, String) = eager.from_list([3, 4])

  sequence
  |> eager.to_observable
  |> rx.map(fn(value) { value + 1 })
  |> rx.to_list
  |> result_payload
}

fn error_payload() -> String {
  let stream: rx.Observable(Int, String) =
    rx.fail("boom")
    |> rx.map_error(fn(error) { "mapped:" <> error })

  case rx.to_list(stream) {
    Ok(values) -> "unexpected:" <> render_ints(values)
    Error(error) -> error
  }
}

fn result_payload(result: Result(List(Int), String)) -> String {
  case result {
    Ok(values) -> render_ints(values)
    Error(error) -> "error:" <> error
  }
}

fn render_ints(values: List(Int)) -> String {
  values
  |> list.map(int.to_string)
  |> string.join(",")
}

fn text_response(status: Int, body: String) -> Response(ResponseData) {
  response.new(status)
  |> response.set_header("content-type", "text/plain; charset=utf-8")
  |> response.set_body(mist.Bytes(bytes_tree.from_string(body)))
}
