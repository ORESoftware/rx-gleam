-module(async_flow_test).
-compile([no_auto_import, nowarn_unused_vars, nowarn_unused_function, nowarn_nomatch, inline]).
-define(FILEPATH, "test/async_flow_test.gleam").
-export([main/0, concat_map_accepts_async_input_while_work_is_running_test/0, merge_map_bounds_concurrency_and_emits_completion_order_test/0, ordered_async_map_processes_concurrently_but_emits_fifo_test/0, projected_error_is_fail_fast_and_late_success_is_ignored_test/0]).
-export_type([source_command/0, worker_event/0, output_event/0]).

-type source_command() :: {emit, integer()} | finish | {fail_source, binary()}.

-type worker_event() :: {worker_started,
        integer(),
        gleam@erlang@process:subject({ok, integer()} | {error, binary()})}.

-type output_event() :: {value, integer()} | {failed, binary()} | completed.

-file("test/async_flow_test.gleam", 25).
-spec main() -> nil.
main() ->
    gleeunit:main().

-file("test/async_flow_test.gleam", 256).
-spec receive_output(gleam@erlang@process:subject(output_event())) -> output_event().
receive_output(Outputs) ->
    Event@1 = case gleam@erlang@process:'receive'(Outputs, 1000) of
        {ok, Event} -> Event;
        _assert_fail ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"async_flow_test"/utf8>>,
                        function => <<"receive_output"/utf8>>,
                        line => 257,
                        value => _assert_fail,
                        start => 7634,
                        'end' => 7701,
                        pattern_start => 7645,
                        pattern_end => 7654})
    end,
    Event@1.

-file("test/async_flow_test.gleam", 246).
-spec output_observer(gleam@erlang@process:subject(output_event())) -> rx:observer(integer(), binary()).
output_observer(Outputs) ->
    rx:observer(
        fun(Value) -> gleam@erlang@process:send(Outputs, {value, Value}) end,
        fun(Reason) -> gleam@erlang@process:send(Outputs, {failed, Reason}) end,
        fun() -> gleam@erlang@process:send(Outputs, completed) end
    ).

-file("test/async_flow_test.gleam", 226).
-spec controlled_future(integer(), gleam@erlang@process:subject(worker_event())) -> rx@future:future(integer(), binary()).
controlled_future(Value, Workers) ->
    rx@future:new(
        fun(Resolve) ->
            Pid = proc_lib:spawn(
                fun() ->
                    Gate = gleam@erlang@process:new_subject(),
                    gleam@erlang@process:send(
                        Workers,
                        {worker_started, Value, Gate}
                    ),
                    case gleam@erlang@process:'receive'(Gate, 5000) of
                        {ok, Result} ->
                            Resolve(Result);

                        {error, nil} ->
                            Resolve({error, <<"worker gate timeout"/utf8>>})
                    end
                end
            ),
            fun() -> gleam@erlang@process:kill(Pid) end
        end
    ).

-file("test/async_flow_test.gleam", 211).
-spec source_loop(
    gleam@erlang@process:subject(source_command()),
    rx:emitter(integer(), binary())
) -> nil.
source_loop(Commands, Emitter) ->
    case gleam@erlang@process:'receive'(Commands, 5000) of
        {error, nil} ->
            rx:error(Emitter, <<"source command timeout"/utf8>>);

        {ok, {emit, Value}} ->
            rx:next(Emitter, Value),
            source_loop(Commands, Emitter);

        {ok, finish} ->
            rx:complete(Emitter);

        {ok, {fail_source, Reason}} ->
            rx:error(Emitter, Reason)
    end.

-file("test/async_flow_test.gleam", 196).
-spec controlled_async_source(
    gleam@erlang@process:subject(gleam@erlang@process:subject(source_command()))
) -> rx:observable(integer(), binary()).
controlled_async_source(Ready) ->
    rx:create(
        fun(Emitter) ->
            Pid = proc_lib:spawn(
                fun() ->
                    Commands = gleam@erlang@process:new_subject(),
                    gleam@erlang@process:send(Ready, Commands),
                    source_loop(Commands, Emitter)
                end
            ),
            fun() -> gleam@erlang@process:kill(Pid) end
        end
    ).

-file("test/async_flow_test.gleam", 29).
-spec concat_map_accepts_async_input_while_work_is_running_test() -> nil.
concat_map_accepts_async_input_while_work_is_running_test() ->
    Ready = gleam@erlang@process:new_subject(),
    Workers = gleam@erlang@process:new_subject(),
    Outputs = gleam@erlang@process:new_subject(),
    Runtime_@1 = case rx@runtime:start() of
        {ok, Runtime_} -> Runtime_;
        _assert_fail ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"async_flow_test"/utf8>>,
                        function => <<"concat_map_accepts_async_input_while_work_is_running_test"/utf8>>,
                        line => 33,
                        value => _assert_fail,
                        start => 578,
                        'end' => 619,
                        pattern_start => 589,
                        pattern_end => 601})
    end,
    Source = controlled_async_source(Ready),
    Mapped = rx@flow:concat_map(
        Source,
        fun(Value) -> controlled_future(Value, Workers) end
    ),
    Subscription@1 = case rx:subscribe(
        Mapped,
        Runtime_@1,
        output_observer(Outputs)
    ) of
        {ok, Subscription} -> Subscription;
        _assert_fail@1 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"async_flow_test"/utf8>>,
                        function => <<"concat_map_accepts_async_input_while_work_is_running_test"/utf8>>,
                        line => 39,
                        value => _assert_fail@1,
                        start => 762,
                        'end' => 852,
                        pattern_start => 773,
                        pattern_end => 789})
    end,
    Commands@1 = case gleam@erlang@process:'receive'(Ready, 1000) of
        {ok, Commands} -> Commands;
        _assert_fail@2 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"async_flow_test"/utf8>>,
                        function => <<"concat_map_accepts_async_input_while_work_is_running_test"/utf8>>,
                        line => 41,
                        value => _assert_fail@2,
                        start => 855,
                        'end' => 923,
                        pattern_start => 866,
                        pattern_end => 878})
    end,
    gleam@erlang@process:send(Commands@1, {emit, 1}),
    Gate1@1 = case gleam@erlang@process:'receive'(Workers, 1000) of
        {ok, {worker_started, 1, Gate1}} -> Gate1;
        _assert_fail@3 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"async_flow_test"/utf8>>,
                        function => <<"concat_map_accepts_async_input_while_work_is_running_test"/utf8>>,
                        line => 44,
                        value => _assert_fail@3,
                        start => 961,
                        'end' => 1050,
                        pattern_start => 972,
                        pattern_end => 999})
    end,
    gleam@erlang@process:send(Commands@1, {emit, 2}),
    gleam@erlang@process:send(Commands@1, {emit, 3}),
    gleam@erlang@process:send(Commands@1, finish),
    case gleam@erlang@process:'receive'(Workers, 20) of
        {error, nil} -> nil;
        _assert_fail@4 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"async_flow_test"/utf8>>,
                        function => <<"concat_map_accepts_async_input_while_work_is_running_test"/utf8>>,
                        line => 53,
                        value => _assert_fail@4,
                        start => 1291,
                        'end' => 1357,
                        pattern_start => 1302,
                        pattern_end => 1312})
    end,
    gleam@erlang@process:send(Gate1@1, {ok, 10}),
    Gate2@1 = case gleam@erlang@process:'receive'(Workers, 1000) of
        {ok, {worker_started, 2, Gate2}} -> Gate2;
        _assert_fail@5 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"async_flow_test"/utf8>>,
                        function => <<"concat_map_accepts_async_input_while_work_is_running_test"/utf8>>,
                        line => 56,
                        value => _assert_fail@5,
                        start => 1391,
                        'end' => 1480,
                        pattern_start => 1402,
                        pattern_end => 1429})
    end,
    _pipe = receive_output(Outputs),
    gleeunit@should:equal(_pipe, {value, 10}),
    gleam@erlang@process:send(Gate2@1, {ok, 20}),
    Gate3@1 = case gleam@erlang@process:'receive'(Workers, 1000) of
        {ok, {worker_started, 3, Gate3}} -> Gate3;
        _assert_fail@6 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"async_flow_test"/utf8>>,
                        function => <<"concat_map_accepts_async_input_while_work_is_running_test"/utf8>>,
                        line => 61,
                        value => _assert_fail@6,
                        start => 1567,
                        'end' => 1656,
                        pattern_start => 1578,
                        pattern_end => 1605})
    end,
    _pipe@1 = receive_output(Outputs),
    gleeunit@should:equal(_pipe@1, {value, 20}),
    gleam@erlang@process:send(Gate3@1, {ok, 30}),
    _pipe@2 = receive_output(Outputs),
    gleeunit@should:equal(_pipe@2, {value, 30}),
    _pipe@3 = receive_output(Outputs),
    gleeunit@should:equal(_pipe@3, completed),
    rx:unsubscribe(Subscription@1),
    rx@runtime:stop(Runtime_@1).

-file("test/async_flow_test.gleam", 178).
-spec receive_worker_pair(gleam@erlang@process:subject(worker_event())) -> {ok,
        {gleam@erlang@process:subject({ok, integer()} | {error, binary()}),
            gleam@erlang@process:subject({ok, integer()} | {error, binary()})}} |
    {error, nil}.
receive_worker_pair(Workers) ->
    case {gleam@erlang@process:'receive'(Workers, 1000),
        gleam@erlang@process:'receive'(Workers, 1000)} of
        {{ok, {worker_started, 1, Gate1}}, {ok, {worker_started, 2, Gate2}}} ->
            {ok, {Gate1, Gate2}};

        {{ok, {worker_started, 2, Gate2@1}}, {ok, {worker_started, 1, Gate1@1}}} ->
            {ok, {Gate1@1, Gate2@1}};

        {_, _} ->
            {error, nil}
    end.

-file("test/async_flow_test.gleam", 73).
-spec merge_map_bounds_concurrency_and_emits_completion_order_test() -> nil.
merge_map_bounds_concurrency_and_emits_completion_order_test() ->
    Ready = gleam@erlang@process:new_subject(),
    Workers = gleam@erlang@process:new_subject(),
    Outputs = gleam@erlang@process:new_subject(),
    Runtime_@1 = case rx@runtime:start() of
        {ok, Runtime_} -> Runtime_;
        _assert_fail ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"async_flow_test"/utf8>>,
                        function => <<"merge_map_bounds_concurrency_and_emits_completion_order_test"/utf8>>,
                        line => 77,
                        value => _assert_fail,
                        start => 2093,
                        'end' => 2134,
                        pattern_start => 2104,
                        pattern_end => 2116})
    end,
    Mapped = begin
        _pipe = controlled_async_source(Ready),
        rx@flow:merge_map(
            _pipe,
            fun(Value) -> controlled_future(Value, Workers) end,
            2
        )
    end,
    Subscription@1 = case rx:subscribe(
        Mapped,
        Runtime_@1,
        output_observer(Outputs)
    ) of
        {ok, Subscription} -> Subscription;
        _assert_fail@1 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"async_flow_test"/utf8>>,
                        function => <<"merge_map_bounds_concurrency_and_emits_completion_order_test"/utf8>>,
                        line => 83,
                        value => _assert_fail@1,
                        start => 2263,
                        'end' => 2353,
                        pattern_start => 2274,
                        pattern_end => 2290})
    end,
    Commands@1 = case gleam@erlang@process:'receive'(Ready, 1000) of
        {ok, Commands} -> Commands;
        _assert_fail@2 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"async_flow_test"/utf8>>,
                        function => <<"merge_map_bounds_concurrency_and_emits_completion_order_test"/utf8>>,
                        line => 85,
                        value => _assert_fail@2,
                        start => 2356,
                        'end' => 2424,
                        pattern_start => 2367,
                        pattern_end => 2379})
    end,
    gleam@erlang@process:send(Commands@1, {emit, 1}),
    gleam@erlang@process:send(Commands@1, {emit, 2}),
    gleam@erlang@process:send(Commands@1, {emit, 3}),
    gleam@erlang@process:send(Commands@1, finish),
    {Gate1@1, Gate2@1} = case receive_worker_pair(Workers) of
        {ok, {Gate1, Gate2}} -> {Gate1, Gate2};
        _assert_fail@3 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"async_flow_test"/utf8>>,
                        function => <<"merge_map_bounds_concurrency_and_emits_completion_order_test"/utf8>>,
                        line => 92,
                        value => _assert_fail@3,
                        start => 2564,
                        'end' => 2625,
                        pattern_start => 2575,
                        pattern_end => 2594})
    end,
    case gleam@erlang@process:'receive'(Workers, 20) of
        {error, nil} -> nil;
        _assert_fail@4 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"async_flow_test"/utf8>>,
                        function => <<"merge_map_bounds_concurrency_and_emits_completion_order_test"/utf8>>,
                        line => 93,
                        value => _assert_fail@4,
                        start => 2628,
                        'end' => 2694,
                        pattern_start => 2639,
                        pattern_end => 2649})
    end,
    gleam@erlang@process:send(Gate2@1, {ok, 20}),
    Gate3@1 = case gleam@erlang@process:'receive'(Workers, 1000) of
        {ok, {worker_started, 3, Gate3}} -> Gate3;
        _assert_fail@5 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"async_flow_test"/utf8>>,
                        function => <<"merge_map_bounds_concurrency_and_emits_completion_order_test"/utf8>>,
                        line => 97,
                        value => _assert_fail@5,
                        start => 2802,
                        'end' => 2891,
                        pattern_start => 2813,
                        pattern_end => 2840})
    end,
    _pipe@1 = receive_output(Outputs),
    gleeunit@should:equal(_pipe@1, {value, 20}),
    gleam@erlang@process:send(Gate1@1, {ok, 10}),
    _pipe@2 = receive_output(Outputs),
    gleeunit@should:equal(_pipe@2, {value, 10}),
    gleam@erlang@process:send(Gate3@1, {ok, 30}),
    _pipe@3 = receive_output(Outputs),
    gleeunit@should:equal(_pipe@3, {value, 30}),
    _pipe@4 = receive_output(Outputs),
    gleeunit@should:equal(_pipe@4, completed),
    rx:unsubscribe(Subscription@1),
    rx@runtime:stop(Runtime_@1).

-file("test/async_flow_test.gleam", 112).
-spec ordered_async_map_processes_concurrently_but_emits_fifo_test() -> nil.
ordered_async_map_processes_concurrently_but_emits_fifo_test() ->
    Ready = gleam@erlang@process:new_subject(),
    Workers = gleam@erlang@process:new_subject(),
    Outputs = gleam@erlang@process:new_subject(),
    Runtime_@1 = case rx@runtime:start() of
        {ok, Runtime_} -> Runtime_;
        _assert_fail ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"async_flow_test"/utf8>>,
                        function => <<"ordered_async_map_processes_concurrently_but_emits_fifo_test"/utf8>>,
                        line => 116,
                        value => _assert_fail,
                        start => 3412,
                        'end' => 3453,
                        pattern_start => 3423,
                        pattern_end => 3435})
    end,
    Mapped = begin
        _pipe = controlled_async_source(Ready),
        rx@flow:map_ordered(
            _pipe,
            fun(Value) -> controlled_future(Value, Workers) end,
            2
        )
    end,
    Subscription@1 = case rx:subscribe(
        Mapped,
        Runtime_@1,
        output_observer(Outputs)
    ) of
        {ok, Subscription} -> Subscription;
        _assert_fail@1 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"async_flow_test"/utf8>>,
                        function => <<"ordered_async_map_processes_concurrently_but_emits_fifo_test"/utf8>>,
                        line => 122,
                        value => _assert_fail@1,
                        start => 3584,
                        'end' => 3674,
                        pattern_start => 3595,
                        pattern_end => 3611})
    end,
    Commands@1 = case gleam@erlang@process:'receive'(Ready, 1000) of
        {ok, Commands} -> Commands;
        _assert_fail@2 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"async_flow_test"/utf8>>,
                        function => <<"ordered_async_map_processes_concurrently_but_emits_fifo_test"/utf8>>,
                        line => 124,
                        value => _assert_fail@2,
                        start => 3677,
                        'end' => 3745,
                        pattern_start => 3688,
                        pattern_end => 3700})
    end,
    gleam@erlang@process:send(Commands@1, {emit, 1}),
    gleam@erlang@process:send(Commands@1, {emit, 2}),
    gleam@erlang@process:send(Commands@1, finish),
    {Gate1@1, Gate2@1} = case receive_worker_pair(Workers) of
        {ok, {Gate1, Gate2}} -> {Gate1, Gate2};
        _assert_fail@3 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"async_flow_test"/utf8>>,
                        function => <<"ordered_async_map_processes_concurrently_but_emits_fifo_test"/utf8>>,
                        line => 130,
                        value => _assert_fail@3,
                        start => 3851,
                        'end' => 3912,
                        pattern_start => 3862,
                        pattern_end => 3881})
    end,
    gleam@erlang@process:send(Gate2@1, {ok, 20}),
    case gleam@erlang@process:'receive'(Outputs, 20) of
        {error, nil} -> nil;
        _assert_fail@4 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"async_flow_test"/utf8>>,
                        function => <<"ordered_async_map_processes_concurrently_but_emits_fifo_test"/utf8>>,
                        line => 134,
                        value => _assert_fail@4,
                        start => 4008,
                        'end' => 4074,
                        pattern_start => 4019,
                        pattern_end => 4029})
    end,
    gleam@erlang@process:send(Gate1@1, {ok, 10}),
    _pipe@1 = receive_output(Outputs),
    gleeunit@should:equal(_pipe@1, {value, 10}),
    _pipe@2 = receive_output(Outputs),
    gleeunit@should:equal(_pipe@2, {value, 20}),
    _pipe@3 = receive_output(Outputs),
    gleeunit@should:equal(_pipe@3, completed),
    rx:unsubscribe(Subscription@1),
    rx@runtime:stop(Runtime_@1).

-file("test/async_flow_test.gleam", 145).
-spec projected_error_is_fail_fast_and_late_success_is_ignored_test() -> nil.
projected_error_is_fail_fast_and_late_success_is_ignored_test() ->
    Ready = gleam@erlang@process:new_subject(),
    Workers = gleam@erlang@process:new_subject(),
    Outputs = gleam@erlang@process:new_subject(),
    Runtime_@1 = case rx@runtime:start() of
        {ok, Runtime_} -> Runtime_;
        _assert_fail ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"async_flow_test"/utf8>>,
                        function => <<"projected_error_is_fail_fast_and_late_success_is_ignored_test"/utf8>>,
                        line => 149,
                        value => _assert_fail,
                        start => 4512,
                        'end' => 4553,
                        pattern_start => 4523,
                        pattern_end => 4535})
    end,
    Mapped = begin
        _pipe = controlled_async_source(Ready),
        rx@flow:merge_map(
            _pipe,
            fun(Value) -> controlled_future(Value, Workers) end,
            2
        )
    end,
    Subscription@1 = case rx:subscribe(
        Mapped,
        Runtime_@1,
        output_observer(Outputs)
    ) of
        {ok, Subscription} -> Subscription;
        _assert_fail@1 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"async_flow_test"/utf8>>,
                        function => <<"projected_error_is_fail_fast_and_late_success_is_ignored_test"/utf8>>,
                        line => 155,
                        value => _assert_fail@1,
                        start => 4682,
                        'end' => 4772,
                        pattern_start => 4693,
                        pattern_end => 4709})
    end,
    Commands@1 = case gleam@erlang@process:'receive'(Ready, 1000) of
        {ok, Commands} -> Commands;
        _assert_fail@2 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"async_flow_test"/utf8>>,
                        function => <<"projected_error_is_fail_fast_and_late_success_is_ignored_test"/utf8>>,
                        line => 157,
                        value => _assert_fail@2,
                        start => 4775,
                        'end' => 4843,
                        pattern_start => 4786,
                        pattern_end => 4798})
    end,
    gleam@erlang@process:send(Commands@1, {emit, 1}),
    gleam@erlang@process:send(Commands@1, {emit, 2}),
    gleam@erlang@process:send(Commands@1, {emit, 3}),
    {Gate1@1, Gate2@1} = case receive_worker_pair(Workers) of
        {ok, {Gate1, Gate2}} -> {Gate1, Gate2};
        _assert_fail@3 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"async_flow_test"/utf8>>,
                        function => <<"projected_error_is_fail_fast_and_late_success_is_ignored_test"/utf8>>,
                        line => 163,
                        value => _assert_fail@3,
                        start => 4950,
                        'end' => 5011,
                        pattern_start => 4961,
                        pattern_end => 4980})
    end,
    gleam@erlang@process:send(Gate2@1, {error, <<"boom"/utf8>>}),
    _pipe@1 = receive_output(Outputs),
    gleeunit@should:equal(_pipe@1, {failed, <<"boom"/utf8>>}),
    gleam@erlang@process:send(Gate1@1, {ok, 10}),
    case gleam@erlang@process:'receive'(Outputs, 20) of
        {error, nil} -> nil;
        _assert_fail@4 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"async_flow_test"/utf8>>,
                        function => <<"projected_error_is_fail_fast_and_late_success_is_ignored_test"/utf8>>,
                        line => 171,
                        value => _assert_fail@4,
                        start => 5300,
                        'end' => 5366,
                        pattern_start => 5311,
                        pattern_end => 5321})
    end,
    case gleam@erlang@process:'receive'(Workers, 20) of
        {error, nil} -> nil;
        _assert_fail@5 ->
            erlang:error(#{gleam_error => let_assert,
                        message => <<"Pattern match failed, no pattern matched the value."/utf8>>,
                        file => <<?FILEPATH/utf8>>,
                        module => <<"async_flow_test"/utf8>>,
                        function => <<"projected_error_is_fail_fast_and_late_success_is_ignored_test"/utf8>>,
                        line => 172,
                        value => _assert_fail@5,
                        start => 5369,
                        'end' => 5435,
                        pattern_start => 5380,
                        pattern_end => 5390})
    end,
    rx:unsubscribe(Subscription@1),
    rx@runtime:stop(Runtime_@1).
