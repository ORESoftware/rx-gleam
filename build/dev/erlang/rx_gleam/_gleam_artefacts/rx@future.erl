-module(rx@future).
-compile([no_auto_import, nowarn_unused_vars, nowarn_unused_function, nowarn_nomatch, inline]).
-define(FILEPATH, "src/rx/future.gleam").
-export([new/1, run/2, await/2, pure/1, fail/1, map/2, from_effect/1, to_effect/1]).
-export_type([future/2]).

-if(?OTP_RELEASE >= 27).
-define(MODULEDOC(Str), -moduledoc(Str)).
-define(DOC(Str), -doc(Str)).
-else.
-define(MODULEDOC(Str), -compile([])).
-define(DOC(Str), -compile([])).
-endif.

-opaque future(GMW, GMX) :: {future, rx@effect:effect(GMW, GMX)}.

-file("src/rx/future.gleam", 16).
-spec new(fun((fun(({ok, GMY} | {error, GMZ}) -> nil)) -> fun(() -> nil))) -> future(GMY, GMZ).
new(Start) ->
    {future, rx@effect:new(Start)}.

-file("src/rx/future.gleam", 22).
-spec run(future(GNE, GNF), fun(({ok, GNE} | {error, GNF}) -> nil)) -> fun(() -> nil).
run(Future, Resolve) ->
    {future, Effect_} = Future,
    rx@effect:run(Effect_, Resolve).

-file("src/rx/future.gleam", 44).
?DOC(
    " `use`-friendly spelling for registering a later continuation.\n"
    "\n"
    " Example:\n"
    "\n"
    " ```gleam\n"
    " use result <- future.await(fetch_user())\n"
    " case result {\n"
    "   Ok(user) -> use_user(user)\n"
    "   Error(reason) -> report(reason)\n"
    " }\n"
    " ```\n"
    "\n"
    " This does not block a BEAM scheduler or the rx Runtime actor. It registers\n"
    " the continuation and returns the Future's physical cancellation callback.\n"
).
-spec await(future(GNK, GNL), fun(({ok, GNK} | {error, GNL}) -> nil)) -> fun(() -> nil).
await(Future, Continue) ->
    run(Future, Continue).

-file("src/rx/future.gleam", 51).
-spec pure(GNQ) -> future(GNQ, any()).
pure(Value) ->
    {future, rx@effect:pure(Value)}.

-file("src/rx/future.gleam", 55).
-spec fail(GNU) -> future(any(), GNU).
fail(Reason) ->
    {future, rx@effect:fail(Reason)}.

-file("src/rx/future.gleam", 59).
-spec map(future(GNY, GNZ), fun((GNY) -> GOC)) -> future(GOC, GNZ).
map(Future, Transform) ->
    {future, Effect_} = Future,
    {future, rx@effect:map(Effect_, Transform)}.

-file("src/rx/future.gleam", 67).
-spec from_effect(rx@effect:effect(GOF, GOG)) -> future(GOF, GOG).
from_effect(Effect_) ->
    {future, Effect_}.

-file("src/rx/future.gleam", 73).
-spec to_effect(future(GOL, GOM)) -> rx@effect:effect(GOL, GOM).
to_effect(Future) ->
    {future, Effect_} = Future,
    Effect_.
