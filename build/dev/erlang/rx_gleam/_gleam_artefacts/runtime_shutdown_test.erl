-module(runtime_shutdown_test).
-compile([no_auto_import, nowarn_unused_vars, nowarn_unused_function, nowarn_nomatch, inline]).
-define(FILEPATH, "test/runtime_shutdown_test.gleam").
-export([main/0, runtime_stop_cancels_active_future_once_test/0, stopped_runtime_rejects_new_subscription_test/0]).
-export_type([worker_event/0]).

-type worker_event() :: {worker_started,
        gleam@erlang@process:subject({ok, integer()} | {error, binary()})}.

-file("test/runtime_shutdown_test.gleam", 13).
-spec main() -> nil.
main() ->
    gleeunit:main().

-file("test/runtime_shutdown_test.gleam", 17).
-spec runtime_stop_cancels_active_future_once_test() -> nil.
runtime_stop_cancels_active_future_once_test() ->
    Workers = gleam@erlang@process:new_subject(),
    Cancelled = gleam@erlang@process:new_subject(),
    Outputs = gleam@erlang@process:new_subject(),
    Runtime_@1 = case rx@runtime:start() of
        {ok, Runtime_} -> Runtime_;
        _assert_fail ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"runtime_shutdown_test"/utf8>>,
                        function => <<"runtime_stop_cancels_active_future_once_test"/utf8>>,
                        line => 21,
                        value => _assert_fail,
                        start => 426,
                        'end' => 467,
                        pattern_start => 437,
                        pattern_end => 449})
    end,
    Source = begin
        _pipe = rx:from_list([1]),
        rx@flow:merge_map(
            _pipe,
            fun(Value) ->
                rx@future:new(
                    fun(Resolve) ->
                        Pid = proc_lib:spawn(
                            fun() ->
                                Gate = gleam@erlang@process:new_subject(),
                                gleam@erlang@process:send(
                                    Workers,
                                    {worker_started, Gate}
                                ),
                                case gleam@erlang@process:'receive'(Gate, 5000) of
                                    {ok, Result} ->
                                        Resolve(Result);

                                    {error, nil} ->
                                        Resolve(
                                            {error, <<"worker timeout"/utf8>>}
                                        )
                                end
                            end
                        ),
                        fun() ->
                            gleam@erlang@process:send(Cancelled, Value),
                            gleam@erlang@process:kill(Pid)
                        end
                    end
                )
            end,
            1
        )
    end,
    case rx:subscribe(
        Source,
        Runtime_@1,
        rx:observer(
            fun(Value@1) -> gleam@erlang@process:send(Outputs, Value@1) end,
            fun(_) -> nil end,
            fun() -> nil end
        )
    ) of
        {ok, _} -> nil;
        _assert_fail@1 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"runtime_shutdown_test"/utf8>>,
                        function => <<"runtime_stop_cancels_active_future_once_test"/utf8>>,
                        line => 46,
                        value => _assert_fail@1,
                        start => 1117,
                        'end' => 1326,
                        pattern_start => 1128,
                        pattern_end => 1145})
    end,
    Gate@2 = case gleam@erlang@process:'receive'(Workers, 1000) of
        {ok, {worker_started, Gate@1}} -> Gate@1;
        _assert_fail@2 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"runtime_shutdown_test"/utf8>>,
                        function => <<"runtime_stop_cancels_active_future_once_test"/utf8>>,
                        line => 56,
                        value => _assert_fail@2,
                        start => 1329,
                        'end' => 1414,
                        pattern_start => 1340,
                        pattern_end => 1363})
    end,
    rx@runtime:stop(Runtime_@1),
    _pipe@1 = gleam@erlang@process:'receive'(Cancelled, 1000),
    gleeunit@should:equal(_pipe@1, {ok, 1}),
    _pipe@2 = gleam@erlang@process:'receive'(Cancelled, 20),
    gleeunit@should:equal(_pipe@2, {error, nil}),
    gleam@erlang@process:send(Gate@2, {ok, 10}),
    _pipe@3 = gleam@erlang@process:'receive'(Outputs, 20),
    gleeunit@should:equal(_pipe@3, {error, nil}).

-file("test/runtime_shutdown_test.gleam", 70).
-spec stopped_runtime_rejects_new_subscription_test() -> nil.
stopped_runtime_rejects_new_subscription_test() ->
    Stopped = gleam@erlang@process:new_subject(),
    Runtime_@1 = case rx@runtime:start() of
        {ok, Runtime_} -> Runtime_;
        _assert_fail ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"runtime_shutdown_test"/utf8>>,
                        function => <<"stopped_runtime_rejects_new_subscription_test"/utf8>>,
                        line => 72,
                        value => _assert_fail,
                        start => 1930,
                        'end' => 1971,
                        pattern_start => 1941,
                        pattern_end => 1953})
    end,
    Live = rx:create(
        fun(_) -> fun() -> gleam@erlang@process:send(Stopped, nil) end end
    ),
    Observer = rx:observer(
        fun(_) -> nil end,
        fun(_) -> nil end,
        fun() -> nil end
    ),
    case rx:subscribe(Live, Runtime_@1, Observer) of
        {ok, _} -> nil;
        _assert_fail@1 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"runtime_shutdown_test"/utf8>>,
                        function => <<"stopped_runtime_rejects_new_subscription_test"/utf8>>,
                        line => 76,
                        value => _assert_fail@1,
                        start => 2149,
                        'end' => 2218,
                        pattern_start => 2160,
                        pattern_end => 2177})
    end,
    rx@runtime:stop(Runtime_@1),
    _pipe = gleam@erlang@process:'receive'(Stopped, 1000),
    gleeunit@should:equal(_pipe, {ok, nil}),
    gleam_erlang_ffi:sleep(10),
    Later = rx:'of'(1),
    _pipe@1 = rx:subscribe(Later, Runtime_@1, Observer),
    gleeunit@should:equal(_pipe@1, {error, runtime_stopped}).
