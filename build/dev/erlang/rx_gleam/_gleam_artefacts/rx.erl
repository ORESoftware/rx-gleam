-module(rx).
-compile([no_auto_import, nowarn_unused_vars, nowarn_unused_function, nowarn_nomatch, inline]).
-define(FILEPATH, "src/rx.gleam").
-export([observer/3, create_checked/1, create_with_runtime/1, create/1, subscribe/3, unsubscribe/1, emit/2, next/2, error/2, complete/1, from_list/1, 'of'/1, empty/0, fail/1, map/2, filter/2, tap/2]).
-export_type([observer/2, subscription/0, observable/2, emitter/2]).

-if(?OTP_RELEASE >= 27).
-define(MODULEDOC(Str), -moduledoc(Str)).
-define(DOC(Str), -doc(Str)).
-else.
-define(MODULEDOC(Str), -compile([])).
-define(DOC(Str), -compile([])).
-endif.

-type observer(GAO, GAP) :: {observer,
        fun((GAO) -> nil),
        fun((GAP) -> nil),
        fun(() -> nil)}.

-opaque subscription() :: {subscription,
        rx@runtime:runtime(),
        rx@runtime:subscription_key()}.

-opaque observable(GAQ, GAR) :: {observable,
        fun((rx@runtime:runtime(), observer(GAQ, GAR)) -> {ok, subscription()} |
            {error, rx@runtime:runtime_error()})}.

-type emitter(GAS, GAT) :: {emitter,
        fun((rx@protocol:notification(GAS, GAT)) -> nil)}.

-file("src/rx.gleam", 27).
-spec observer(fun((GAU) -> nil), fun((GAV) -> nil), fun(() -> nil)) -> observer(GAU, GAV).
observer(On_next, On_error, On_complete) ->
    {observer, On_next, On_error, On_complete}.

-file("src/rx.gleam", 206).
-spec notify(observer(GEH, GEI), rx@protocol:notification(GEH, GEI)) -> nil.
notify(Observer_, Notification) ->
    case Notification of
        {on_next, Value} ->
            (erlang:element(2, Observer_))(Value);

        {on_error, Reason} ->
            (erlang:element(3, Observer_))(Reason);

        on_complete ->
            (erlang:element(4, Observer_))()
    end.

-file("src/rx.gleam", 57).
?DOC(" Runtime-aware Observable construction with typed subscription failure.\n").
-spec create_checked(
    fun((rx@runtime:runtime(), emitter(GBK, GBL)) -> {ok, fun(() -> nil)} |
        {error, rx@runtime:runtime_error()})
) -> observable(GBK, GBL).
create_checked(Producer) ->
    {observable,
        fun(Runtime_, Observer_) -> case rx@runtime:register(Runtime_) of
                {error, Reason} ->
                    {error, Reason};

                {ok, Key} ->
                    Emitter = {emitter,
                        fun(Notification) ->
                            rx@runtime:dispatch(
                                Runtime_,
                                Key,
                                rx@protocol:kind(Notification),
                                fun() -> notify(Observer_, Notification) end
                            )
                        end},
                    case Producer(Runtime_, Emitter) of
                        {error, Reason@1} ->
                            rx@runtime:cancel(Runtime_, Key),
                            {error, Reason@1};

                        {ok, Teardown} ->
                            rx@runtime:set_teardown(Runtime_, Key, Teardown),
                            {ok, {subscription, Runtime_, Key}}
                    end
            end end}.

-file("src/rx.gleam", 50).
?DOC(
    " Create an Observable whose producer can access the owning Runtime.\n"
    "\n"
    " This is intended for advanced operators that need to register serialized\n"
    " state with the runtime while still accepting asynchronous source emissions.\n"
).
-spec create_with_runtime(
    fun((rx@runtime:runtime(), emitter(GBE, GBF)) -> fun(() -> nil))
) -> observable(GBE, GBF).
create_with_runtime(Producer) ->
    create_checked(
        fun(Runtime_, Emitter) -> {ok, Producer(Runtime_, Emitter)} end
    ).

-file("src/rx.gleam", 40).
?DOC(
    " Create an Observable from a callback producer.\n"
    "\n"
    " The producer may call the emitter now or at any later time. This is the\n"
    " primitive used to adapt asynchronous push sources such as sockets, queues,\n"
    " actor messages, timers, and callback APIs.\n"
).
-spec create(fun((emitter(GAY, GAZ)) -> fun(() -> nil))) -> observable(GAY, GAZ).
create(Producer) ->
    create_with_runtime(fun(_, Emitter) -> Producer(Emitter) end).

-file("src/rx.gleam", 87).
-spec subscribe(observable(GBS, GBT), rx@runtime:runtime(), observer(GBS, GBT)) -> {ok,
        subscription()} |
    {error, rx@runtime:runtime_error()}.
subscribe(Observable, Runtime_, Observer_) ->
    {observable, Subscribe_} = Observable,
    Subscribe_(Runtime_, Observer_).

-file("src/rx.gleam", 96).
-spec unsubscribe(subscription()) -> nil.
unsubscribe(Subscription) ->
    {subscription, Runtime_, Key} = Subscription,
    rx@runtime:cancel(Runtime_, Key).

-file("src/rx.gleam", 101).
-spec emit(emitter(GCA, GCB), rx@protocol:notification(GCA, GCB)) -> nil.
emit(Emitter, Notification) ->
    {emitter, Send} = Emitter,
    Send(Notification).

-file("src/rx.gleam", 109).
-spec next(emitter(GCG, any()), GCG) -> nil.
next(Emitter, Value) ->
    emit(Emitter, {on_next, Value}).

-file("src/rx.gleam", 113).
-spec error(emitter(any(), GCL), GCL) -> nil.
error(Emitter, Reason) ->
    emit(Emitter, {on_error, Reason}).

-file("src/rx.gleam", 117).
-spec complete(emitter(any(), any())) -> nil.
complete(Emitter) ->
    emit(Emitter, on_complete).

-file("src/rx.gleam", 147).
-spec emit_list(emitter(GDJ, any()), list(GDJ)) -> nil.
emit_list(Emitter, Values) ->
    case Values of
        [] ->
            nil;

        [First | Rest] ->
            next(Emitter, First),
            emit_list(Emitter, Rest)
    end.

-file("src/rx.gleam", 139).
-spec from_list(list(GDE)) -> observable(GDE, any()).
from_list(Values) ->
    create(
        fun(Emitter) ->
            emit_list(Emitter, Values),
            complete(Emitter),
            fun() -> nil end
        end
    ).

-file("src/rx.gleam", 121).
-spec 'of'(GCS) -> observable(GCS, any()).
'of'(Value) ->
    from_list([Value]).

-file("src/rx.gleam", 125).
-spec empty() -> observable(any(), any()).
empty() ->
    create(
        fun(Emitter) ->
            complete(Emitter),
            fun() -> nil end
        end
    ).

-file("src/rx.gleam", 132).
-spec fail(GDA) -> observable(any(), GDA).
fail(Reason) ->
    create(
        fun(Emitter) ->
            error(Emitter, Reason),
            fun() -> nil end
        end
    ).

-file("src/rx.gleam", 157).
-spec map(observable(GDO, GDP), fun((GDO) -> GDS)) -> observable(GDS, GDP).
map(Observable, Transform) ->
    {observable,
        fun(Runtime_, Downstream) ->
            subscribe(
                Observable,
                Runtime_,
                {observer,
                    fun(Value) ->
                        (erlang:element(2, Downstream))(Transform(Value))
                    end,
                    erlang:element(3, Downstream),
                    erlang:element(4, Downstream)}
            )
        end}.

-file("src/rx.gleam", 174).
-spec filter(observable(GDV, GDW), fun((GDV) -> boolean())) -> observable(GDV, GDW).
filter(Observable, Predicate) ->
    {observable,
        fun(Runtime_, Downstream) ->
            subscribe(
                Observable,
                Runtime_,
                {observer, fun(Value) -> case Predicate(Value) of
                            true ->
                                (erlang:element(2, Downstream))(Value);

                            false ->
                                nil
                        end end, erlang:element(3, Downstream), erlang:element(
                        4,
                        Downstream
                    )}
            )
        end}.

-file("src/rx.gleam", 196).
-spec tap(observable(GEB, GEC), fun((GEB) -> nil)) -> observable(GEB, GEC).
tap(Observable, Inspect) ->
    map(
        Observable,
        fun(Value) ->
            Inspect(Value),
            Value
        end
    ).
