-module(rx@flow).
-compile([no_auto_import, nowarn_unused_vars, nowarn_unused_function, nowarn_nomatch, inline]).
-define(FILEPATH, "src/rx/flow.gleam").
-export([concat_map/2, merge_map/3, map_ordered/3, map_async/2, filter_async_concurrent/3, filter_async/2, from_future/1]).
-export_type([decision/1]).

-if(?OTP_RELEASE >= 27).
-define(MODULEDOC(Str), -moduledoc(Str)).
-define(DOC(Str), -doc(Str)).
-else.
-define(MODULEDOC(Str), -compile([])).
-define(DOC(Str), -compile([])).
-endif.

-type decision(GPX) :: {decision, GPX, boolean()}.

-file("src/rx/flow.gleam", 101).
-spec map_concurrent(
    rx:observable(GSE, GSF),
    fun((GSE) -> rx@future:future(GSI, GSF)),
    integer(),
    rx@runtime:flow_order()
) -> rx:observable(GSI, GSF).
map_concurrent(Source, Project, Concurrency, Order) ->
    rx:create_checked(
        fun(Runtime_, Emitter) ->
            case rx@runtime:register_flow(
                Runtime_,
                Concurrency,
                Order,
                fun() -> rx:complete(Emitter) end
            ) of
                {error, Reason} ->
                    {error, Reason};

                {ok, Flow_key} ->
                    Upstream = rx:subscribe(
                        Source,
                        Runtime_,
                        rx:observer(
                            fun(Value) ->
                                rx@runtime:enqueue_flow(
                                    Runtime_,
                                    Flow_key,
                                    fun(Sequence) ->
                                        rx@future:run(
                                            Project(Value),
                                            fun(Result) -> case Result of
                                                    {ok, Output} ->
                                                        rx@runtime:complete_flow(
                                                            Runtime_,
                                                            Flow_key,
                                                            Sequence,
                                                            flow_success,
                                                            fun() ->
                                                                rx:next(
                                                                    Emitter,
                                                                    Output
                                                                )
                                                            end
                                                        );

                                                    {error, Reason@1} ->
                                                        rx@runtime:complete_flow(
                                                            Runtime_,
                                                            Flow_key,
                                                            Sequence,
                                                            flow_failure,
                                                            fun() ->
                                                                rx:error(
                                                                    Emitter,
                                                                    Reason@1
                                                                )
                                                            end
                                                        )
                                                end end
                                        )
                                    end
                                )
                            end,
                            fun(Reason@2) ->
                                rx@runtime:fail_flow_input(
                                    Runtime_,
                                    Flow_key,
                                    fun() -> rx:error(Emitter, Reason@2) end
                                )
                            end,
                            fun() ->
                                rx@runtime:finish_flow_input(Runtime_, Flow_key)
                            end
                        )
                    ),
                    case Upstream of
                        {error, Reason@3} ->
                            rx@runtime:cancel_flow(Runtime_, Flow_key),
                            {error, Reason@3};

                        {ok, Subscription} ->
                            {ok,
                                fun() ->
                                    rx:unsubscribe(Subscription),
                                    rx@runtime:cancel_flow(Runtime_, Flow_key)
                                end}
                    end
            end
        end
    ).

-file("src/rx/flow.gleam", 10).
?DOC(
    " Strict FIFO async mapping.\n"
    "\n"
    " Source values may arrive at arbitrary future times. At most one projected\n"
    " Future runs at once; later source values remain queued until the active\n"
    " Future resolves. This is the ReactiveX `concatMap` scheduling policy.\n"
).
-spec concat_map(
    rx:observable(GPY, GPZ),
    fun((GPY) -> rx@future:future(GQC, GPZ))
) -> rx:observable(GQC, GPZ).
concat_map(Source, Project) ->
    map_concurrent(Source, Project, 1, input_order).

-file("src/rx/flow.gleam", 22).
?DOC(
    " Concurrent async mapping with FIFO dispatch and completion-order emission.\n"
    "\n"
    " `concurrency` bounds the number of projected Futures that can be active at\n"
    " once. New source values continue arriving asynchronously and are buffered in\n"
    " FIFO order until a slot becomes available.\n"
).
-spec merge_map(
    rx:observable(GQH, GQI),
    fun((GQH) -> rx@future:future(GQL, GQI)),
    integer()
) -> rx:observable(GQL, GQI).
merge_map(Source, Project, Concurrency) ->
    map_concurrent(Source, Project, Concurrency, completion_order).

-file("src/rx/flow.gleam", 34).
?DOC(
    " Concurrent async mapping that preserves input order at emission time.\n"
    "\n"
    " Work may finish out of order, but successful results are buffered until all\n"
    " earlier input sequence numbers have emitted.\n"
).
-spec map_ordered(
    rx:observable(GQQ, GQR),
    fun((GQQ) -> rx@future:future(GQU, GQR)),
    integer()
) -> rx:observable(GQU, GQR).
map_ordered(Source, Project, Concurrency) ->
    map_concurrent(Source, Project, Concurrency, input_order).

-file("src/rx/flow.gleam", 43).
?DOC(" Async map with exactly one operation in flight.\n").
-spec map_async(
    rx:observable(GQZ, GRA),
    fun((GQZ) -> rx@future:future(GRD, GRA))
) -> rx:observable(GRD, GRA).
map_async(Source, Project) ->
    concat_map(Source, Project).

-file("src/rx/flow.gleam", 59).
?DOC(" Evaluate async predicates concurrently while preserving source order.\n").
-spec filter_async_concurrent(
    rx:observable(GRQ, GRR),
    fun((GRQ) -> rx@future:future(boolean(), GRR)),
    integer()
) -> rx:observable(GRQ, GRR).
filter_async_concurrent(Source, Predicate, Concurrency) ->
    Decisions = map_ordered(
        Source,
        fun(Value) ->
            rx@future:map(
                Predicate(Value),
                fun(Keep) -> {decision, Value, Keep} end
            )
        end,
        Concurrency
    ),
    _pipe = Decisions,
    _pipe@1 = rx:filter(_pipe, fun(Decision) -> erlang:element(3, Decision) end),
    rx:map(_pipe@1, fun(Decision@1) -> erlang:element(2, Decision@1) end).

-file("src/rx/flow.gleam", 51).
?DOC(" Async filter with exactly one predicate Future in flight.\n").
-spec filter_async(
    rx:observable(GRI, GRJ),
    fun((GRI) -> rx@future:future(boolean(), GRJ))
) -> rx:observable(GRI, GRJ).
filter_async(Source, Predicate) ->
    filter_async_concurrent(Source, Predicate, 1).

-file("src/rx/flow.gleam", 81).
?DOC(" Convert one Future into a single-value Observable.\n").
-spec from_future(rx@future:future(GRY, GRZ)) -> rx:observable(GRY, GRZ).
from_future(Future_) ->
    rx:create(
        fun(Emitter) -> rx@future:run(Future_, fun(Result) -> case Result of
                        {ok, Value} ->
                            rx:next(Emitter, Value),
                            rx:complete(Emitter);

                        {error, Reason} ->
                            rx:error(Emitter, Reason)
                    end end) end
    ).
