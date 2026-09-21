# rx-gleam server-side use cases

These examples focus on BEAM/server workloads rather than browser UI. Every `rx`, `rx/flow`, and `rx/future` call shown below exists in the current library. Functions such as `db_write`, `dedupe_first_seen`, `retrying_fetch`, or `group_router_submit` are application adapters: they may use actors, ETS, sockets, database clients, timers, ports, or FFI, and expose that work as `future.Future(value, error)` or as callback registration/teardown.

The important boundary is deliberate: **rx-gleam owns one serialized runtime actor; application state and application concurrency remain application-owned.**

## 1. Async queue with an async processing step

**Problem:** Requests arrive faster than a side effect should run. A database write, migration step, payment mutation, or external command must start strictly one-at-a-time in FIFO order.

**Solution:** Use `flow.concat_map`. Source items may arrive asynchronously, but only one projected `Future` is active at once.

```gleam
import rx
import rx/flow
import rx/future

type Job {
  Job(id: Int)
}

type Ack {
  Ack(id: Int)
}

fn process_job(job: Job) -> future.Future(Ack, String) {
  // Application-owned async adapter.
  todo
}

fn pipeline(jobs: rx.Observable(Job, String)) {
  jobs
  |> flow.concat_map(process_job)
}
```

Use this for ordered writes and command queues. If job `1` takes three seconds, job `2` does not start until job `1` resolves.

## 2. De-duplicating requests or stream items with an in-memory set

**Problem:** A queue, webhook provider, or retrying client may deliver the same logical request more than once. You want to keep a `Set`/`Dict` of request IDs in memory and process only the first occurrence.

**Solution:** Keep the set in an application-owned state holder and expose `first_seen(id)` as a `Future(Bool, error)`. Then use `filter_async` or `filter_async_concurrent`.

```gleam
import rx
import rx/flow
import rx/future

type Request {
  Request(id: String, body: String)
}

fn first_seen(id: String) -> future.Future(Bool, String) {
  // Ask an application-owned actor / ETS table / cache.
  // Resolve Ok(True) once, then Ok(False) for duplicates.
  todo
}

fn unique_requests(requests: rx.Observable(Request, String)) {
  requests
  |> flow.filter_async_concurrent(
    fn(request) {
      let Request(id:, body: _) = request
      first_seen(id)
    },
    16,
  )
}
```

If dedupe order itself must be strictly serialized, use `flow.filter_async` instead of the concurrent form.

## 3. Grouping requests by tenant, partition, account, or resource key

**Problem:** Requests from many tenants arrive on one ingress stream. Work for one tenant must preserve that tenant's ordering, while unrelated tenants should progress independently.

**Solution:** Keep the key-to-worker/key-to-queue map in an application-owned group router. Project each stream item into the appropriate group and let the router provide per-key FIFO semantics.

```gleam
import rx
import rx/flow
import rx/future

type Request {
  Request(tenant_id: String, payload: String)
}

type Result {
  Result(tenant_id: String)
}

fn group_router_submit(
  tenant_id: String,
  request: Request,
) -> future.Future(Result, String) {
  // Application-owned Dict(tenant_id, queue/worker).
  todo
}

fn grouped_processing(requests: rx.Observable(Request, String)) {
  requests
  |> flow.merge_map(
    fn(request) {
      let Request(tenant_id:, payload: _) = request
      group_router_submit(tenant_id, request)
    },
    32,
  )
}
```

This is usually a better server-side interpretation of `groupBy` than exposing long-lived nested observables: the grouping registry owns resource lifetime and rx-gleam owns composition.

## 4. Merging multiple server-side push sources

**Problem:** NATS messages, socket notifications, timer callbacks, and internal hooks should all enter the same downstream pipeline.

**Solution:** Adapt the external producers at one ingress boundary and have all of them emit into the same `Emitter`.

```gleam
import rx

type IngressEvent {
  QueueMessage(String)
  SocketMessage(String)
  Tick(Int)
}

fn merged_ingress(register_queue, register_socket, register_tick) {
  rx.create(fn(emitter) {
    let cancel_queue = register_queue(fn(message) {
      rx.next(emitter, QueueMessage(message))
    })

    let cancel_socket = register_socket(fn(message) {
      rx.next(emitter, SocketMessage(message))
    })

    let cancel_tick = register_tick(fn(number) {
      rx.next(emitter, Tick(number))
    })

    fn() {
      cancel_queue()
      cancel_socket()
      cancel_tick()
    }
  })
}
```

All downstream observer-visible work is serialized through the same rx runtime actor even when the external producers run concurrently.

A first-class observable-to-observable `merge` operator is still a useful future API; this ingress pattern avoids pretending that operator already exists.

## 5. Rebasing heterogeneous streams onto one canonical stream

**Problem:** Different protocols describe the same domain event differently. HTTP webhooks, broker messages, and internal commands should be normalized before business logic sees them.

**Solution:** Rebase each source onto one canonical event type at the adapter boundary, then feed the normalized events into one observable.

```gleam
import rx

type CanonicalEvent {
  UserChanged(user_id: String)
  InvoicePaid(invoice_id: String)
  RebuildRequested(scope: String)
}

fn canonical_ingress(register_webhooks, register_broker, register_commands) {
  rx.create(fn(emitter) {
    let cancel_webhooks = register_webhooks(fn(webhook) {
      rx.next(emitter, UserChanged(webhook.user_id))
    })

    let cancel_broker = register_broker(fn(message) {
      rx.next(emitter, InvoicePaid(message.invoice_id))
    })

    let cancel_commands = register_commands(fn(command) {
      rx.next(emitter, RebuildRequested(command.scope))
    })

    fn() {
      cancel_webhooks()
      cancel_broker()
      cancel_commands()
    }
  })
}
```

The difference from plain merging is semantic: merging combines sources; rebasing first converts them to one canonical contract.

## 6. Bounding concurrent outbound RPC or HTTP work

**Problem:** You have thousands of work items but must not open thousands of simultaneous outbound requests.

**Solution:** Use `flow.merge_map` with an explicit concurrency bound.

```gleam
requests
|> flow.merge_map(call_remote_service, 32)
```

At most 32 projected Futures are active. Completion-order results are emitted as soon as they finish.

This bounds **active async work**, not total buffered input. A producer that permanently outruns the consumer can still grow the flow queue; source-level backpressure/load shedding is a separate concern.

## 7. Running work concurrently while preserving input order

**Problem:** Enrichment calls may complete out of order, but downstream persistence or protocol output must match request order.

**Solution:** Use `flow.map_ordered`.

```gleam
records
|> flow.map_ordered(enrich_record, 16)
```

Sixteen enrichments may run concurrently. If item `7` finishes before item `6`, item `7` is buffered until `6` can emit.

## 8. Async authorization or admission filtering

**Problem:** Every request needs an asynchronous policy check before it can enter the business pipeline.

**Solution:** Make the policy check a `Future(Bool, error)` and use `filter_async_concurrent`.

```gleam
requests
|> flow.filter_async_concurrent(
  fn(request) { authorize(request.principal, request.action) },
  24,
)
```

Successful `True` decisions pass the original request downstream. `False` is dropped. Errors fail the flow.

## 9. Serializing transactional writes while accepting requests asynchronously

**Problem:** A service may accept commands concurrently, but a specific persistence path must not overlap transactions.

**Solution:** Keep ingress asynchronous and serialize only the write stage.

```gleam
commands
|> rx.map(validate_command)
|> flow.concat_map(write_transaction)
```

This is useful for append-only ledgers, migration steps, sequential file writes, or APIs with strict mutation ordering.

## 10. API calls with retry and exponential backoff

**Problem:** A dependency is temporarily unavailable. You want retries with application-specific backoff without introducing an Rx scheduler abstraction.

**Solution:** Put retry/timer policy inside the application Future adapter, then compose it normally.

```gleam
import rx/flow
import rx/future

fn retrying_fetch(request) -> future.Future(Response, FetchError) {
  // Application adapter owns attempts, timers, jitter, and cancellation.
  todo
}

requests
|> flow.merge_map(retrying_fetch, 12)
```

rx-gleam does not need to own timers or scheduler threads to compose retrying async work.

## 11. Batching telemetry or log events

**Problem:** Writing every telemetry event separately is wasteful. You want an in-memory accumulator that flushes at a size or time threshold.

**Solution:** Keep batch state and timers in an application-owned batching actor, and submit stream items to it through an ordered Future boundary.

```gleam
fn add_to_batch(event) -> future.Future(BatchAck, String) {
  // Actor owns List(Event), count, timer, and flush policy.
  todo
}

telemetry
|> flow.concat_map(add_to_batch)
```

A future first-class `buffer` operator can make this syntax shorter, but the ownership model should stay the same: batching state is explicit and cancellation-aware.

## 12. Timeout with fallback

**Problem:** A dependency may hang beyond your service SLA. After a deadline, you want to cancel the underlying attempt and return cached or degraded data.

**Solution:** Implement the deadline race in the application Future adapter and expose one typed result to the flow.

```gleam
fn fetch_with_fallback(key) -> future.Future(Value, FetchError) {
  // Race remote fetch vs timer; cancel loser; use cache on timeout.
  todo
}

keys
|> flow.merge_map(fetch_with_fallback, 20)
```

This keeps timeout policy close to the resource being cancelled rather than hiding it behind a global Rx scheduler.

## 13. Joining independent dependencies for one request

**Problem:** A request needs a profile, permissions, and account limits. Those calls can happen in parallel, but downstream code needs one combined context.

**Solution:** Expose the application-level join as one `Future(Context, error)` and use `map_ordered` or `merge_map` depending on output ordering requirements.

```gleam
fn load_context(request) -> future.Future(Context, String) {
  // Start independent application calls, resolve when all required data exists.
  todo
}

requests
|> flow.map_ordered(load_context, 16)
```

A future `zip`/`combine_latest` API would compose observables directly; this pattern is preferable today when the dependencies are one-shot request/response operations.

## 14. Pausing and resuming server-side consumption

**Problem:** Operations needs a maintenance switch that temporarily rejects or suppresses work without tearing down the ingress connection.

**Solution:** Ask an application-owned gate asynchronously and use it as an admission predicate.

```gleam
fn gate_is_open(request) -> future.Future(Bool, String) {
  // Read an actor, ETS flag, config service, or feature flag cache.
  todo
}

incoming
|> flow.filter_async_concurrent(gate_is_open, 32)
```

If paused items must be buffered rather than dropped, put that queue in the gate/dispatcher instead of pretending filtering provides buffering semantics.

## 15. Centralized server state / reducer actor

**Problem:** Many events update one logical in-memory state machine: counters, connection state, routing tables, cache metadata, or workflow state.

**Solution:** Let an application actor own the state and submit events to it sequentially through `concat_map`.

```gleam
fn apply_event(event) -> future.Future(StateSnapshot, String) {
  // Actor receives event, transitions immutable state, returns snapshot.
  todo
}

events
|> flow.concat_map(apply_event)
```

This is the server-side analogue of a reducer/store pattern without pretending Gleam needs a browser-style UI state library.

## 16. Heartbeat / watchdog monitoring

**Problem:** A peer, worker, or device should be considered unhealthy if heartbeats stop arriving before a deadline.

**Solution:** Keep timer ownership in the connection/watchdog adapter and expose status changes as an observable push source.

```gleam
import rx

type PeerStatus {
  Alive
  TimedOut
}

fn peer_status(register_watchdog) {
  rx.create(fn(emitter) {
    register_watchdog(fn(status) {
      case status {
        Alive -> rx.next(emitter, Alive)
        TimedOut -> {
          rx.next(emitter, TimedOut)
          rx.complete(emitter)
        }
      }
    })
  })
}
```

The adapter owns timer reset/cancellation; rx-gleam owns serialized delivery and terminal protocol rules.

## 17. Dependent sequential service calls

**Problem:** You must load a user first, then use `company_id` from that result to fetch the company.

**Solution:** Convert the first Future to an observable and flatten the dependent Future with `concat_map`.

```gleam
import rx/flow

fetch_user(user_id)
|> flow.from_future
|> flow.concat_map(fn(user) { fetch_company(user.company_id) })
```

This is a server-friendly equivalent of a dependent `switchMap`/`flatMap` chain when the first operation emits exactly one value.

## 18. Interleaving progress events and a final result

**Problem:** A long-running upload, export, build, or migration needs to emit progress updates and then one terminal result.

**Solution:** Adapt the application's callback protocol with `rx.create`.

```gleam
import rx

type UploadEvent {
  Progress(percent: Int)
  Finished(url: String)
}

fn upload_stream(start_upload) {
  rx.create(fn(emitter) {
    start_upload(fn(update) {
      case update {
        Progress(percent) -> rx.next(emitter, Progress(percent))
        Finished(url) -> {
          rx.next(emitter, Finished(url))
          rx.complete(emitter)
        }
      }
    })
  })
}
```

Late callbacks after completion are rejected by the runtime protocol state machine.

## 19. Sliding-window metrics and moving aggregates

**Problem:** You need rolling latency, error-rate, queue-depth, or throughput metrics over the last N samples.

**Solution:** Keep the rolling window in an application-owned metrics actor and serialize updates through the reactive pipeline.

```gleam
fn update_window(sample) -> future.Future(MetricSnapshot, String) {
  // Actor keeps the last N samples and computes the next snapshot.
  todo
}

samples
|> flow.concat_map(update_window)
```

This keeps mutable-looking state explicit as an immutable actor state transition. A future `scan`/window operator can reduce the ceremony for purely reactive state.

## 20. Connection-scoped cancellation and teardown

**Problem:** A WebSocket, SSE request, queue consumer, or client session disconnects. All source registrations and active projected work associated with that subscription should stop.

**Solution:** Return physical teardown from `rx.create`, retain the returned `Subscription`, and unsubscribe when the owning connection ends.

```gleam
let source =
  rx.create(fn(emitter) {
    let cancel_source = register_source(fn(value) {
      rx.next(emitter, value)
    })

    fn() { cancel_source() }
  })

let assert Ok(subscription) =
  source
  |> flow.merge_map(handle_value, 8)
  |> rx.subscribe(runtime_, observer)

// Connection closed:
rx.unsubscribe(subscription)
```

Cancellation is idempotent. Flow cancellation discards queued work, invokes cancellation callbacks for active Futures, and suppresses late completions.

## What should become first-class next?

Several server patterns above are already clean with `Future` plus application-owned state. Others are common enough to deserve native operators after their state machines and cancellation semantics are specified:

- observable-to-observable `merge` / `concat`;
- `distinct_until_changed` and bounded/global `distinct`;
- keyed `group_by` or partition routing;
- `scan` / state accumulation;
- `buffer` / windowing;
- `retry` / recovery policy combinators;
- `timeout`;
- `zip` / `combine_latest`;
- `switch_map` and `exhaust_map`.

Those should not be added as surface-only helpers. Any operator with state, timers, cancellation, nested subscriptions, or reordering should extend the executable reference model and formal/conformance coverage first.