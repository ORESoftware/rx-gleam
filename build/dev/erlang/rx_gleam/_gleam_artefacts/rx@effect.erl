-module(rx@effect).
-compile([no_auto_import, nowarn_unused_vars, nowarn_unused_function, nowarn_nomatch, inline]).
-define(FILEPATH, "src/rx/effect.gleam").
-export([new/1, run/2, pure/1, fail/1, map/2, to_observable/1]).
-export_type([effect/2]).

-if(?OTP_RELEASE >= 27).
-define(MODULEDOC(Str), -moduledoc(Str)).
-define(DOC(Str), -doc(Str)).
-else.
-define(MODULEDOC(Str), -compile([])).
-define(DOC(Str), -compile([])).
-endif.

-opaque effect(GJQ, GJR) :: {effect,
        fun((fun(({ok, GJQ} | {error, GJR}) -> nil)) -> fun(() -> nil))}.

-file("src/rx/effect.gleam", 12).
-spec new(fun((fun(({ok, GJS} | {error, GJT}) -> nil)) -> fun(() -> nil))) -> effect(GJS, GJT).
new(Start) ->
    {effect, Start}.

-file("src/rx/effect.gleam", 18).
-spec run(effect(GJY, GJZ), fun(({ok, GJY} | {error, GJZ}) -> nil)) -> fun(() -> nil).
run(Effect, Resolve) ->
    {effect, Start} = Effect,
    Start(Resolve).

-file("src/rx/effect.gleam", 26).
-spec pure(GKE) -> effect(GKE, any()).
pure(Value) ->
    {effect,
        fun(Resolve) ->
            Resolve({ok, Value}),
            fun() -> nil end
        end}.

-file("src/rx/effect.gleam", 33).
-spec fail(GKI) -> effect(any(), GKI).
fail(Reason) ->
    {effect,
        fun(Resolve) ->
            Resolve({error, Reason}),
            fun() -> nil end
        end}.

-file("src/rx/effect.gleam", 40).
-spec map(effect(GKM, GKN), fun((GKM) -> GKQ)) -> effect(GKQ, GKN).
map(Effect, Transform) ->
    {effect, fun(Resolve) -> run(Effect, fun(Result) -> case Result of
                        {ok, Value} ->
                            Resolve({ok, Transform(Value)});

                        {error, Reason} ->
                            Resolve({error, Reason})
                    end end) end}.

-file("src/rx/effect.gleam", 59).
?DOC(
    " Convert one effect result into an Observable.\n"
    "\n"
    " The effect's cancellation function becomes the Observable subscription\n"
    " teardown. Multiple or late resolver calls are still governed by the same\n"
    " runtime protocol as every other Observable producer.\n"
).
-spec to_observable(effect(GKT, GKU)) -> rx:observable(GKT, GKU).
to_observable(Effect) ->
    rx:create(fun(Emitter) -> run(Effect, fun(Result) -> case Result of
                        {ok, Value} ->
                            rx:next(Emitter, Value),
                            rx:complete(Emitter);

                        {error, Reason} ->
                            rx:error(Emitter, Reason)
                    end end) end).
