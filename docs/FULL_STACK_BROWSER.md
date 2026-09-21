# Full-stack Gleam: browser JavaScript, WASM interop, and rx-gleam

This document describes how a full-stack Gleam application can use `rx-gleam` on both the BEAM server and in the browser.

The important distinction is:

- **Gleam application code compiles directly to Erlang or JavaScript.** For the browser, the supported application target is JavaScript.
- **Gleam does not currently expose WebAssembly as an application-code compilation target.** The Gleam compiler itself can run as WebAssembly in a browser, but it still compiles Gleam source to JavaScript or Erlang.
- **WebAssembly is still useful alongside browser-targeted Gleam** for CPU-heavy kernels implemented in Rust/C/etc. Those modules can be loaded from JavaScript and called from Gleam through JavaScript FFI.

For `rx-gleam`, the near-term frontend path should therefore be **Gleam -> JavaScript**, with optional WASM modules underneath selected effects or operators.

Upstream references:

- Gleam CLI targets: https://gleam.run/documentation/command-line-reference/
- `gleam.toml` target configuration: https://gleam.run/documentation/gleam-toml-reference/
- JavaScript externals / FFI: https://gleam.run/documentation/externals/
- Gleam JavaScript compatibility: https://gleam.run/documentation/compatibility-reference/
- Gleam compiler-in-WASM background: https://gleam.run/news/v0.33-exhaustive-gleam/
- Lustre browser applications: https://hexdocs.pm/lustre/
- `gleam_fetch`: https://gleam-fetch.hexdocs.pm/

## 1. Recommended architecture

Use separate Gleam packages for server, browser, and shared code:

```text
my-app/
├── apps/
│   ├── server/
│   │   ├── gleam.toml          # target = "erlang"
│   │   └── src/
│   │       └── server.gleam
│   │
│   └── web/
│       ├── gleam.toml          # target = "javascript"
│       └── src/
│           └── web.gleam
│
└── packages/
    └── shared/
        ├── gleam.toml          # target-neutral
        └── src/
            ├── contracts.gleam
            ├── domain.gleam
            └── codecs.gleam
```

The server can run Mist, Wisp, Cowboy, or another Gleam HTTP adapter on the BEAM. The browser package compiles to ECMAScript modules and can use Lustre, plain DOM bindings, or JavaScript FFI.

The shared package should contain only cross-target Gleam code. Avoid `gleam/erlang/*`, `gleam/otp/*`, browser-only JavaScript externals, and target-specific dependencies there.

This gives us one language and one set of domain contracts across both ends without pretending that the browser has OTP semantics.

## 2. Compiling Gleam for the browser

A browser package can set:

```toml
name = "my_app_web"
version = "0.1.0"
target = "javascript"

gleam = ">= 1.18.1"
```

Then build it with:

```sh
gleam build --target javascript
```

Gleam emits JavaScript ES modules. The current JavaScript target requires ECMAScript 2022 or newer; a normal JS bundler can lower that further when older browsers must be supported.

A generated Gleam module can also be imported from ordinary JavaScript. For example, a Gleam module `src/app.gleam` is emitted as an ES module that can be imported as `app.mjs` from the generated package tree.

A practical production pipeline is:

```text
Gleam source
    |
    v
`gleam build --target javascript`
    |
    v
ES modules (.mjs)
    |
    v
optional bundler/minifier
    |
    v
browser assets served by the Gleam server/CDN
```

The bundler is optional from Gleam's point of view. Use one when the application needs asset hashing, code splitting, CSS processing, older-browser lowering, or an existing frontend asset pipeline.

## 3. Where rx-gleam stands today

The current `rx-gleam` runtime intentionally has a strong BEAM invariant:

```text
one rx/runtime.Runtime
        |
        v
exactly one OTP actor
        |
        v
serialized observer callbacks and async-flow state
```

That implementation imports `gleam/otp/actor`, `gleam/erlang/process`, and Erlang references. Consequently, the current actor-backed modules are **Erlang-target modules**, not browser modules.

This is good server-side architecture and should not be weakened merely to make the package compile in JavaScript.

The browser implementation should preserve the semantic contract while changing the mechanism:

```text
BEAM                              Browser
----                              -------
OTP actor                         one serialized JS dispatcher
actor mailbox                     FIFO event queue / microtask drain
process reference                 monotonically unique JS-side key
Future completion -> actor        Promise/callback -> dispatcher
teardown stored by actor          teardown stored by dispatcher
no hidden worker pool             no hidden Web Worker pool
```

The invariant is the same: **one serialization boundary per Runtime**. The platform implementation differs.

## 4. Proposed rx-gleam target split

To make `rx-gleam` genuinely full-stack, split target-independent semantics from target-specific execution.

Recommended module direction:

```text
src/
├── rx.gleam
├── rx/
│   ├── protocol.gleam           # target-neutral
│   ├── lifecycle.gleam          # target-neutral
│   ├── eager.gleam              # target-neutral finite operators
│   ├── future.gleam             # target-neutral Future contract
│   ├── effect.gleam             # target-neutral Effect contract
│   ├── flow_model.gleam         # target-neutral queue/order state machine
│   │
│   ├── runtime/
│   │   ├── beam.gleam           # OTP actor implementation
│   │   └── browser.gleam        # serialized JS event-loop implementation
│   │
│   └── browser/
│       ├── promise.gleam        # Promise <-> Future adapters
│       ├── event_target.gleam   # DOM/EventTarget adapters
│       ├── websocket.gleam      # WebSocket -> Observable adapter
│       └── fetch.gleam          # HTTP/Fetch -> Future/Observable adapter
```

One detail matters for the current repository: `rx/eager.gleam` imports the top-level `rx` module because it exposes `eager.to_observable`. That bridge pulls the BEAM runtime into the dependency graph. To make eager operators browser-safe, move the pure eager implementation away from the runtime bridge, for example:

```text
rx/eager.gleam                  # pure, target-neutral
rx/bridge/eager_observable.gleam
```

or make the bridge depend on a target-neutral Observable interface rather than directly on the BEAM runtime implementation.

The goal is that this command eventually succeeds in CI:

```sh
gleam check --target javascript
```

without removing the BEAM actor implementation or compromising server semantics.

## 5. Browser Runtime semantics

The browser Runtime should not invent a scheduler abstraction that behaves differently from the server library. It should implement the same state machine with a browser-native serialized dispatcher.

Conceptually:

```text
incoming event
    |
    v
runtime.enqueue(message)
    |
    +-- if drain already scheduled: return
    |
    +-- schedule one microtask
            |
            v
       drain FIFO queue
            |
            v
       protocol/lifecycle transition
            |
            v
       observer callback
```

A JavaScript FFI module can supply the tiny platform primitives required to schedule the queue, preferably with `queueMicrotask` or a resolved Promise.

Do **not** use a Web Worker merely to imitate an OTP process. Web Workers should remain application-owned concurrency, just as BEAM processes outside the runtime remain application-owned concurrency on the server.

This maintains the existing design principle:

> ReactiveX defines composition; the host platform defines concurrency.

On BEAM the host platform is OTP/processes. In the browser it is the JavaScript event loop, Promises, callbacks, Web APIs, and optionally Web Workers.

## 6. HTTP and RPC from browser Gleam

For REST-style browser calls, `gleam_fetch` provides Gleam bindings to the browser Fetch API.

A frontend can therefore look like:

```text
Gleam browser component
        |
        +--> rx/browser/fetch
        |       |
        |       v
        |   Future/Observable
        |       |
        v       v
     fetch() -> /rest/...
              /rpc/...
              /graphql/...
```

The server stays an Erlang-target Gleam application.

For a full-stack application with several interfaces, keep the same server prefixes used by the backend:

```text
/rest      ordinary HTTP resources
/rpc       generated or authored RPC
/graphql   GraphQL HTTP requests
/ws        WebSocket connections
```

The frontend Rx adapters should be transport adapters, not alternate business logic. Shared request/response types and codecs should live in the target-neutral shared package.

## 7. WebSockets as Observables

WebSockets are a particularly natural frontend `rx-gleam` adapter:

```text
browser WebSocket.onmessage
          |
          v
      rx.next(...)
          |
          v
serialized browser Runtime
          |
          v
      operators
          |
          v
     UI/model update
```

The adapter should map:

- `message` -> `OnNext`
- terminal socket error -> `OnError`
- `close` -> `OnComplete` when appropriate
- unsubscribe -> remove listeners + close/cancel according to ownership policy

The adapter must still obey the existing Rx grammar:

```text
Next* (Error | Complete)?
```

Late browser callbacks after cancellation or terminal notification must be ignored exactly as late BEAM messages are ignored today.

## 8. Lustre integration

Lustre is a strong fit for a full-stack Gleam application because it supports browser SPAs/components and server-side patterns while remaining Gleam-first.

The recommended ownership boundary is:

```text
rx-gleam Observable
       |
       v
Lustre message
       |
       v
update(model, message)
       |
       v
new model + Lustre effects
```

Rx should orchestrate event streams, transport streams, cancellation, async ordering, and transformation. Lustre should continue to own UI model/view/update semantics.

Avoid making the Rx observer directly mutate arbitrary DOM state. Feed typed events into the application state machine instead.

## 9. What WASM means here

There are two different "Gleam + WASM" stories and they should not be confused.

### 9.1 The Gleam compiler itself in WASM

The Gleam compiler can itself run as a WebAssembly module. This is how browser-based compilation/playground scenarios can compile Gleam source without a server.

That does **not** mean application modules such as `rx/runtime.gleam` are compiled into `.wasm` for deployment. The compiler running inside Wasm still emits one of Gleam's application targets.

### 9.2 Application-owned WASM kernels

For production browser applications, use WASM when a specific function benefits from it:

```text
Gleam -> JavaScript
    |
    v
JS FFI adapter
    |
    v
WebAssembly module
    |
    v
Result / Promise
    |
    v
rx Future / Observable
```

Examples include:

- compression/decompression;
- cryptography implemented by an audited WASM library;
- audio/DSP transforms;
- image processing;
- parsers;
- large numeric transforms;
- domain-specific Rust libraries already built to `wasm32`.

The WASM module should be treated like any other application-owned asynchronous effect. `rx-gleam` should compose its completion/cancellation rather than hide a separate scheduler inside the Rx library.

## 10. Suggested full-stack development flow

Server:

```sh
cd apps/server
gleam run --target erlang
```

Browser:

```sh
cd apps/web
gleam build --target javascript
```

During development, a frontend dev server/bundler can watch the generated ES modules while the Gleam server handles API/WebSocket traffic.

For production:

```text
1. build shared package
2. build browser package with `--target javascript`
3. bundle/minify/hash static browser assets if desired
4. build the server on the Erlang target
5. serve browser assets from the server, CDN, or object storage
6. browser connects back to `/rest`, `/rpc`, `/graphql`, and `/ws`
```

## 11. CI requirements for rx-gleam frontend support

When browser support is implemented, add a JavaScript lane rather than replacing the BEAM lanes:

```sh
gleam format --check src test

gleam check --target erlang
gleam test --target erlang

gleam check --target javascript
gleam test --target javascript
```

Also add a browser-consumer fixture analogous to the current external Mist consumer. It should:

1. install `rx-gleam` as an external dependency;
2. build a JavaScript-target Gleam application;
3. bundle or directly load the generated ES modules in a headless browser;
4. exercise eager operators;
5. exercise the browser Runtime;
6. adapt a Promise into a Future/Observable;
7. connect to a real test WebSocket endpoint;
8. verify terminal/cancellation behavior;
9. verify no callback arrives after unsubscribe;
10. verify server and browser implementations satisfy the same conformance traces.

The same generated protocol traces used by the BEAM runtime should be run against the browser runtime. This is more valuable than merely checking that both targets compile.

## 12. Recommended implementation order

1. Make `protocol`, `lifecycle`, eager collection operators, and the flow reference model explicitly target-neutral.
2. Remove the top-level `rx` dependency from the pure eager implementation.
3. Define the smallest internal Runtime contract required by Observable/flow code.
4. Keep the existing OTP actor as the BEAM implementation of that contract.
5. Implement a one-queue browser Runtime with JavaScript FFI only for event-loop scheduling/identity primitives.
6. Run the same protocol and lifecycle conformance suite against both runtimes.
7. Add Promise adapters.
8. Add Fetch and WebSocket adapters.
9. Add a Lustre example application.
10. Add optional WASM-effect examples only after the JavaScript/browser path is green.

That sequence gets us to a real full-stack `rx-gleam` much faster than attempting a direct Gleam-to-WASM runtime first.

## Bottom line

For a full-stack Gleam application, the intended architecture is:

```text
                    shared Gleam contracts/domain
                       /                 \
                      /                   \
                     v                     v
        Gleam -> Erlang/BEAM          Gleam -> JavaScript
              server                       browser
                |                            |
        rx runtime = OTP actor      rx runtime = serial JS queue
                |                            |
       HTTP/RPC/GraphQL/WS <--------> Fetch/WebSocket
                                             |
                                      optional JS FFI
                                             |
                                      optional WASM
```

Use JavaScript as the browser compilation target. Preserve the existing single-serialization-boundary semantics with a browser Runtime. Use WebAssembly as an optional implementation detail for specialized effects, not as a substitute for Gleam's supported JavaScript frontend target.