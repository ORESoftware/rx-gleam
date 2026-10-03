-module(runtime_test).
-compile([no_auto_import, nowarn_unused_vars, nowarn_unused_function, nowarn_nomatch, inline]).
-define(FILEPATH, "test/runtime_test.gleam").
-export([main/0, runtime_rejects_post_terminal_notifications_test/0, cancellation_is_idempotent_and_teardown_runs_once_test/0, runtime_stop_runs_active_subscription_teardown_once_test/0, observer_can_subscribe_reentrantly_on_same_runtime_test/0]).
-export_type([event/0]).

-type event() :: {value, integer()} |
    completed |
    {protocol_violation, rx@protocol:protocol_error()} |
    duplicate_teardown |
    nested_subscribe_failed |
    torn_down.

-file("test/runtime_test.gleam", 17).
-spec main() -> nil.
main() ->
    gleeunit:main().

-file("test/runtime_test.gleam", 21).
-spec runtime_rejects_post_terminal_notifications_test() -> nil.
runtime_rejects_post_terminal_notifications_test() ->
    Events = gleam@erlang@process:new_subject(),
    Runtime_@1 = case rx@runtime:start_checked(
        fun(Diagnostic) -> case Diagnostic of
                {protocol_violation, Reason} ->
                    gleam@erlang@process:send(
                        Events,
                        {protocol_violation, Reason}
                    );

                duplicate_teardown_registration ->
                    gleam@erlang@process:send(Events, duplicate_teardown)
            end end
    ) of
        {ok, Runtime_} -> Runtime_;
        _assert_fail ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"runtime_test"/utf8>>,
                        function => <<"runtime_rejects_post_terminal_notifications_test"/utf8>>,
                        line => 23,
                        value => _assert_fail,
                        start => 402,
                        'end' => 711,
                        pattern_start => 413,
                        pattern_end => 425})
    end,
    Source = rx:create(
        fun(Emitter) ->
            rx:next(Emitter, 1),
            rx:complete(Emitter),
            rx:next(Emitter, 2),
            fun() -> gleam@erlang@process:send(Events, torn_down) end
        end
    ),
    case rx:subscribe(
        Source,
        Runtime_@1,
        rx:observer(
            fun(Value) -> gleam@erlang@process:send(Events, {value, Value}) end,
            fun(_) -> nil end,
            fun() -> gleam@erlang@process:send(Events, completed) end
        )
    ) of
        {ok, _} -> nil;
        _assert_fail@1 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"runtime_test"/utf8>>,
                        function => <<"runtime_rejects_post_terminal_notifications_test"/utf8>>,
                        line => 41,
                        value => _assert_fail@1,
                        start => 919,
                        'end' => 1162,
                        pattern_start => 930,
                        pattern_end => 947})
    end,
    _pipe = gleam@erlang@process:'receive'(Events, 1000),
    gleeunit@should:equal(_pipe, {ok, {value, 1}}),
    _pipe@1 = gleam@erlang@process:'receive'(Events, 1000),
    gleeunit@should:equal(_pipe@1, {ok, completed}),
    _pipe@2 = gleam@erlang@process:'receive'(Events, 1000),
    gleeunit@should:equal(
        _pipe@2,
        {ok, {protocol_violation, notification_after_termination}}
    ),
    _pipe@3 = gleam@erlang@process:'receive'(Events, 1000),
    gleeunit@should:equal(_pipe@3, {ok, torn_down}),
    rx@runtime:stop(Runtime_@1).

-file("test/runtime_test.gleam", 64).
-spec cancellation_is_idempotent_and_teardown_runs_once_test() -> nil.
cancellation_is_idempotent_and_teardown_runs_once_test() ->
    Events = gleam@erlang@process:new_subject(),
    Runtime_@1 = case rx@runtime:start() of
        {ok, Runtime_} -> Runtime_;
        _assert_fail ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"runtime_test"/utf8>>,
                        function => <<"cancellation_is_idempotent_and_teardown_runs_once_test"/utf8>>,
                        line => 66,
                        value => _assert_fail,
                        start => 1659,
                        'end' => 1700,
                        pattern_start => 1670,
                        pattern_end => 1682})
    end,
    Source = rx:create(
        fun(_) -> fun() -> gleam@erlang@process:send(Events, torn_down) end end
    ),
    Subscription@1 = case rx:subscribe(
        Source,
        Runtime_@1,
        rx:observer(fun(_) -> nil end, fun(_) -> nil end, fun() -> nil end)
    ) of
        {ok, Subscription} -> Subscription;
        _assert_fail@1 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"runtime_test"/utf8>>,
                        function => <<"cancellation_is_idempotent_and_teardown_runs_once_test"/utf8>>,
                        line => 71,
                        value => _assert_fail@1,
                        start => 1813,
                        'end' => 1959,
                        pattern_start => 1824,
                        pattern_end => 1840})
    end,
    rx:unsubscribe(Subscription@1),
    rx:unsubscribe(Subscription@1),
    _pipe = gleam@erlang@process:'receive'(Events, 1000),
    gleeunit@should:equal(_pipe, {ok, torn_down}),
    _pipe@1 = gleam@erlang@process:'receive'(Events, 20),
    gleeunit@should:equal(_pipe@1, {error, nil}),
    rx@runtime:stop(Runtime_@1).

-file("test/runtime_test.gleam", 89).
-spec runtime_stop_runs_active_subscription_teardown_once_test() -> nil.
runtime_stop_runs_active_subscription_teardown_once_test() ->
    Events = gleam@erlang@process:new_subject(),
    Runtime_@1 = case rx@runtime:start() of
        {ok, Runtime_} -> Runtime_;
        _assert_fail ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"runtime_test"/utf8>>,
                        function => <<"runtime_stop_runs_active_subscription_teardown_once_test"/utf8>>,
                        line => 91,
                        value => _assert_fail,
                        start => 2312,
                        'end' => 2353,
                        pattern_start => 2323,
                        pattern_end => 2335})
    end,
    Source = rx:create(
        fun(_) -> fun() -> gleam@erlang@process:send(Events, torn_down) end end
    ),
    case rx:subscribe(
        Source,
        Runtime_@1,
        rx:observer(fun(_) -> nil end, fun(_) -> nil end, fun() -> nil end)
    ) of
        {ok, _} -> nil;
        _assert_fail@1 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"runtime_test"/utf8>>,
                        function => <<"runtime_stop_runs_active_subscription_teardown_once_test"/utf8>>,
                        line => 95,
                        value => _assert_fail@1,
                        start => 2465,
                        'end' => 2612,
                        pattern_start => 2476,
                        pattern_end => 2493})
    end,
    rx@runtime:stop(Runtime_@1),
    _pipe = gleam@erlang@process:'receive'(Events, 1000),
    gleeunit@should:equal(_pipe, {ok, torn_down}),
    _pipe@1 = gleam@erlang@process:'receive'(Events, 20),
    gleeunit@should:equal(_pipe@1, {error, nil}).

-file("test/runtime_test.gleam", 110).
-spec observer_can_subscribe_reentrantly_on_same_runtime_test() -> nil.
observer_can_subscribe_reentrantly_on_same_runtime_test() ->
    Events = gleam@erlang@process:new_subject(),
    Runtime_@1 = case rx@runtime:start() of
        {ok, Runtime_} -> Runtime_;
        _assert_fail ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"runtime_test"/utf8>>,
                        function => <<"observer_can_subscribe_reentrantly_on_same_runtime_test"/utf8>>,
                        line => 112,
                        value => _assert_fail,
                        start => 2901,
                        'end' => 2942,
                        pattern_start => 2912,
                        pattern_end => 2924})
    end,
    Outer = rx:'of'(1),
    case rx:subscribe(
        Outer,
        Runtime_@1,
        rx:observer(
            fun(Value) ->
                Inner = rx:'of'(Value + 1),
                case rx:subscribe(
                    Inner,
                    Runtime_@1,
                    rx:observer(
                        fun(Inner_value) ->
                            gleam@erlang@process:send(
                                Events,
                                {value, Inner_value}
                            )
                        end,
                        fun(_) -> nil end,
                        fun() -> nil end
                    )
                ) of
                    {ok, _} ->
                        nil;

                    {error, _} ->
                        gleam@erlang@process:send(
                            Events,
                            nested_subscribe_failed
                        )
                end
            end,
            fun(_) -> nil end,
            fun() -> nil end
        )
    ) of
        {ok, _} -> nil;
        _assert_fail@1 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"runtime_test"/utf8>>,
                        function => <<"observer_can_subscribe_reentrantly_on_same_runtime_test"/utf8>>,
                        line => 115,
                        value => _assert_fail@1,
                        start => 2997,
                        'end' => 3657,
                        pattern_start => 3008,
                        pattern_end => 3031})
    end,
    _pipe = gleam@erlang@process:'receive'(Events, 1000),
    gleeunit@should:equal(_pipe, {ok, {value, 2}}),
    _pipe@1 = gleam@erlang@process:'receive'(Events, 20),
    gleeunit@should:equal(_pipe@1, {error, nil}),
    rx@runtime:stop(Runtime_@1).
