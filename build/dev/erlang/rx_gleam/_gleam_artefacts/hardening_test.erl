-module(hardening_test).
-compile([no_auto_import, nowarn_unused_vars, nowarn_unused_function, nowarn_nomatch, inline]).
-define(FILEPATH, "test/hardening_test.gleam").
-export([main/0, synchronous_terminal_source_runs_late_teardown_once_test/0, late_emission_after_unsubscribe_is_ignored_test/0, operators_never_run_after_terminal_error_test/0, multiple_subscriptions_are_isolated_on_one_runtime_test/0]).
-export_type([event/0]).

-type event() :: {value, integer()} |
    {failed, binary()} |
    completed |
    torn_down |
    {transformed, integer()} |
    {protocol_violation, rx@protocol:protocol_error()}.

-file("test/hardening_test.gleam", 17).
-spec main() -> nil.
main() ->
    gleeunit:main().

-file("test/hardening_test.gleam", 151).
-spec 'receive'(gleam@erlang@process:subject(event())) -> event().
'receive'(Events) ->
    Event@1 = case gleam@erlang@process:'receive'(Events, 1000) of
        {ok, Event} -> Event;
        _assert_fail ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"hardening_test"/utf8>>,
                        function => <<"receive"/utf8>>,
                        line => 152,
                        value => _assert_fail,
                        start => 4283,
                        'end' => 4349,
                        pattern_start => 4294,
                        pattern_end => 4303})
    end,
    Event@1.

-file("test/hardening_test.gleam", 143).
-spec observer(gleam@erlang@process:subject(event())) -> rx:observer(integer(), binary()).
observer(Events) ->
    rx:observer(
        fun(Value) -> gleam@erlang@process:send(Events, {value, Value}) end,
        fun(Reason) -> gleam@erlang@process:send(Events, {failed, Reason}) end,
        fun() -> gleam@erlang@process:send(Events, completed) end
    ).

-file("test/hardening_test.gleam", 21).
-spec synchronous_terminal_source_runs_late_teardown_once_test() -> nil.
synchronous_terminal_source_runs_late_teardown_once_test() ->
    Events = gleam@erlang@process:new_subject(),
    Runtime_@1 = case rx@runtime:start() of
        {ok, Runtime_} -> Runtime_;
        _assert_fail ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"hardening_test"/utf8>>,
                        function => <<"synchronous_terminal_source_runs_late_teardown_once_test"/utf8>>,
                        line => 23,
                        value => _assert_fail,
                        start => 402,
                        'end' => 443,
                        pattern_start => 413,
                        pattern_end => 425})
    end,
    Source = rx:create(
        fun(Emitter) ->
            rx:next(Emitter, 1),
            rx:complete(Emitter),
            fun() -> gleam@erlang@process:send(Events, torn_down) end
        end
    ),
    Subscription@1 = case rx:subscribe(Source, Runtime_@1, observer(Events)) of
        {ok, Subscription} -> Subscription;
        _assert_fail@1 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"hardening_test"/utf8>>,
                        function => <<"synchronous_terminal_source_runs_late_teardown_once_test"/utf8>>,
                        line => 32,
                        value => _assert_fail@1,
                        start => 625,
                        'end' => 703,
                        pattern_start => 636,
                        pattern_end => 652})
    end,
    _pipe = 'receive'(Events),
    gleeunit@should:equal(_pipe, {value, 1}),
    _pipe@1 = 'receive'(Events),
    gleeunit@should:equal(_pipe@1, completed),
    _pipe@2 = 'receive'(Events),
    gleeunit@should:equal(_pipe@2, torn_down),
    rx:unsubscribe(Subscription@1),
    _pipe@3 = gleam@erlang@process:'receive'(Events, 20),
    gleeunit@should:equal(_pipe@3, {error, nil}),
    rx@runtime:stop(Runtime_@1).

-file("test/hardening_test.gleam", 46).
-spec late_emission_after_unsubscribe_is_ignored_test() -> nil.
late_emission_after_unsubscribe_is_ignored_test() ->
    Ready = gleam@erlang@process:new_subject(),
    Events = gleam@erlang@process:new_subject(),
    Runtime_@1 = case rx@runtime:start() of
        {ok, Runtime_} -> Runtime_;
        _assert_fail ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"hardening_test"/utf8>>,
                        function => <<"late_emission_after_unsubscribe_is_ignored_test"/utf8>>,
                        line => 49,
                        value => _assert_fail,
                        start => 1163,
                        'end' => 1204,
                        pattern_start => 1174,
                        pattern_end => 1186})
    end,
    Source = rx:create(
        fun(Emitter) ->
            gleam@erlang@process:send(Ready, Emitter),
            fun() -> gleam@erlang@process:send(Events, torn_down) end
        end
    ),
    Subscription@1 = case rx:subscribe(Source, Runtime_@1, observer(Events)) of
        {ok, Subscription} -> Subscription;
        _assert_fail@1 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"hardening_test"/utf8>>,
                        function => <<"late_emission_after_unsubscribe_is_ignored_test"/utf8>>,
                        line => 57,
                        value => _assert_fail@1,
                        start => 1368,
                        'end' => 1446,
                        pattern_start => 1379,
                        pattern_end => 1395})
    end,
    Emitter@2 = case gleam@erlang@process:'receive'(Ready, 1000) of
        {ok, Emitter@1} -> Emitter@1;
        _assert_fail@2 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"hardening_test"/utf8>>,
                        function => <<"late_emission_after_unsubscribe_is_ignored_test"/utf8>>,
                        line => 58,
                        value => _assert_fail@2,
                        start => 1449,
                        'end' => 1516,
                        pattern_start => 1460,
                        pattern_end => 1471})
    end,
    rx:unsubscribe(Subscription@1),
    _pipe = 'receive'(Events),
    gleeunit@should:equal(_pipe, torn_down),
    rx:next(Emitter@2, 99),
    rx:complete(Emitter@2),
    _pipe@1 = gleam@erlang@process:'receive'(Events, 20),
    gleeunit@should:equal(_pipe@1, {error, nil}),
    rx@runtime:stop(Runtime_@1).

-file("test/hardening_test.gleam", 71).
-spec operators_never_run_after_terminal_error_test() -> nil.
operators_never_run_after_terminal_error_test() ->
    Events = gleam@erlang@process:new_subject(),
    Diagnostics = gleam@erlang@process:new_subject(),
    Runtime_@1 = case rx@runtime:start_checked(
        fun(Diagnostic) -> case Diagnostic of
                {protocol_violation, Reason} ->
                    gleam@erlang@process:send(
                        Diagnostics,
                        {protocol_violation, Reason}
                    );

                duplicate_teardown_registration ->
                    nil
            end end
    ) of
        {ok, Runtime_} -> Runtime_;
        _assert_fail ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"hardening_test"/utf8>>,
                        function => <<"operators_never_run_after_terminal_error_test"/utf8>>,
                        line => 74,
                        value => _assert_fail,
                        start => 1881,
                        'end' => 2149,
                        pattern_start => 1892,
                        pattern_end => 1904})
    end,
    Source = rx:create(
        fun(Emitter) ->
            rx:next(Emitter, 1),
            rx:error(Emitter, <<"boom"/utf8>>),
            rx:next(Emitter, 2),
            fun() -> gleam@erlang@process:send(Events, torn_down) end
        end
    ),
    Mapped = begin
        _pipe = Source,
        rx:map(
            _pipe,
            fun(Value) ->
                gleam@erlang@process:send(Events, {transformed, Value}),
                Value * 10
            end
        )
    end,
    case rx:subscribe(Mapped, Runtime_@1, observer(Events)) of
        {ok, _} -> nil;
        _assert_fail@1 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"hardening_test"/utf8>>,
                        function => <<"operators_never_run_after_terminal_error_test"/utf8>>,
                        line => 100,
                        value => _assert_fail@1,
                        start => 2631,
                        'end' => 2714,
                        pattern_start => 2642,
                        pattern_end => 2659})
    end,
    _pipe@1 = 'receive'(Events),
    gleeunit@should:equal(_pipe@1, {transformed, 1}),
    _pipe@2 = 'receive'(Events),
    gleeunit@should:equal(_pipe@2, {value, 10}),
    _pipe@3 = 'receive'(Events),
    gleeunit@should:equal(_pipe@3, {failed, <<"boom"/utf8>>}),
    _pipe@4 = 'receive'(Diagnostics),
    gleeunit@should:equal(
        _pipe@4,
        {protocol_violation, notification_after_termination}
    ),
    _pipe@5 = 'receive'(Events),
    gleeunit@should:equal(_pipe@5, torn_down),
    _pipe@6 = gleam@erlang@process:'receive'(Events, 20),
    gleeunit@should:equal(_pipe@6, {error, nil}),
    rx@runtime:stop(Runtime_@1).

-file("test/hardening_test.gleam", 115).
-spec multiple_subscriptions_are_isolated_on_one_runtime_test() -> nil.
multiple_subscriptions_are_isolated_on_one_runtime_test() ->
    Left = gleam@erlang@process:new_subject(),
    Right = gleam@erlang@process:new_subject(),
    Runtime_@1 = case rx@runtime:start() of
        {ok, Runtime_} -> Runtime_;
        _assert_fail ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"hardening_test"/utf8>>,
                        function => <<"multiple_subscriptions_are_isolated_on_one_runtime_test"/utf8>>,
                        line => 118,
                        value => _assert_fail,
                        start => 3247,
                        'end' => 3288,
                        pattern_start => 3258,
                        pattern_end => 3270})
    end,
    Source = rx:from_list([1, 2, 3]),
    case rx:subscribe(Source, Runtime_@1, observer(Left)) of
        {ok, _} -> nil;
        _assert_fail@1 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"hardening_test"/utf8>>,
                        function => <<"multiple_subscriptions_are_isolated_on_one_runtime_test"/utf8>>,
                        line => 121,
                        value => _assert_fail@1,
                        start => 3359,
                        'end' => 3445,
                        pattern_start => 3370,
                        pattern_end => 3392})
    end,
    case rx:subscribe(
        begin
            _pipe = Source,
            rx:map(_pipe, fun(Value) -> Value * 10 end)
        end,
        Runtime_@1,
        observer(Right)
    ) of
        {ok, _} -> nil;
        _assert_fail@2 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"hardening_test"/utf8>>,
                        function => <<"multiple_subscriptions_are_isolated_on_one_runtime_test"/utf8>>,
                        line => 123,
                        value => _assert_fail@2,
                        start => 3448,
                        'end' => 3597,
                        pattern_start => 3459,
                        pattern_end => 3482})
    end,
    _pipe@1 = 'receive'(Left),
    gleeunit@should:equal(_pipe@1, {value, 1}),
    _pipe@2 = 'receive'(Left),
    gleeunit@should:equal(_pipe@2, {value, 2}),
    _pipe@3 = 'receive'(Left),
    gleeunit@should:equal(_pipe@3, {value, 3}),
    _pipe@4 = 'receive'(Left),
    gleeunit@should:equal(_pipe@4, completed),
    _pipe@5 = 'receive'(Right),
    gleeunit@should:equal(_pipe@5, {value, 10}),
    _pipe@6 = 'receive'(Right),
    gleeunit@should:equal(_pipe@6, {value, 20}),
    _pipe@7 = 'receive'(Right),
    gleeunit@should:equal(_pipe@7, {value, 30}),
    _pipe@8 = 'receive'(Right),
    gleeunit@should:equal(_pipe@8, completed),
    rx@runtime:stop(Runtime_@1).
