-module(rx@protocol).
-compile([no_auto_import, nowarn_unused_vars, nowarn_unused_function, nowarn_nomatch, inline]).
-define(FILEPATH, "src/rx/protocol.gleam").
-export([kind/1, transition/2, validate/1]).
-export_type([notification/2, kind/0, phase/0, protocol_error/0]).

-type notification(FKP, FKQ) :: {on_next, FKP} | {on_error, FKQ} | on_complete.

-type kind() :: next_kind | error_kind | complete_kind.

-type phase() :: open | terminated.

-type protocol_error() :: notification_after_termination | duplicate_termination.

-file("src/rx/protocol.gleam", 23).
-spec kind(notification(any(), any())) -> kind().
kind(Notification) ->
    case Notification of
        {on_next, _} ->
            next_kind;

        {on_error, _} ->
            error_kind;

        on_complete ->
            complete_kind
    end.

-file("src/rx/protocol.gleam", 31).
-spec transition(phase(), kind()) -> {ok, phase()} | {error, protocol_error()}.
transition(Phase, Event) ->
    case {Phase, Event} of
        {open, next_kind} ->
            {ok, open};

        {open, error_kind} ->
            {ok, terminated};

        {open, complete_kind} ->
            {ok, terminated};

        {terminated, next_kind} ->
            {error, notification_after_termination};

        {terminated, error_kind} ->
            {error, duplicate_termination};

        {terminated, complete_kind} ->
            {error, duplicate_termination}
    end.

-file("src/rx/protocol.gleam", 48).
-spec validate_from(phase(), list(notification(any(), any()))) -> {ok, phase()} |
    {error, protocol_error()}.
validate_from(Phase, Notifications) ->
    case Notifications of
        [] ->
            {ok, Phase};

        [First | Rest] ->
            case transition(Phase, kind(First)) of
                {ok, Next} ->
                    validate_from(Next, Rest);

                {error, Error} ->
                    {error, Error}
            end
    end.

-file("src/rx/protocol.gleam", 42).
-spec validate(list(notification(any(), any()))) -> {ok, phase()} |
    {error, protocol_error()}.
validate(Notifications) ->
    validate_from(open, Notifications).
