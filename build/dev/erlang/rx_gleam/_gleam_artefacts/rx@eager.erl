-module(rx@eager).
-compile([no_auto_import, nowarn_unused_vars, nowarn_unused_function, nowarn_nomatch, inline]).
-define(FILEPATH, "src/rx/eager.gleam").
-export([from_list/1, fail/1, 'of'/1, from_result/1, empty/0, to_observable/1, to_result/1, map/2, map_error/2, filter/2, tap/2, take/2, skip/2, scan/3, flat_map/2, append/2, fold/3]).
-export_type([eager/2]).

-if(?OTP_RELEASE >= 27).
-define(MODULEDOC(Str), -moduledoc(Str)).
-define(DOC(Str), -doc(Str)).
-else.
-define(MODULEDOC(Str), -compile([])).
-define(DOC(Str), -compile([])).
-endif.

-opaque eager(HWK, HWL) :: {eager, {ok, list(HWK)} | {error, HWL}}.

-file("src/rx/eager.gleam", 13).
-spec from_list(list(HWM)) -> eager(HWM, any()).
from_list(Values) ->
    {eager, {ok, Values}}.

-file("src/rx/eager.gleam", 32).
-spec fail(HXF) -> eager(any(), HXF).
fail(Error) ->
    {eager, {error, Error}}.

-file("src/rx/eager.gleam", 24).
-spec 'of'(HWX) -> eager(HWX, any()).
'of'(Value) ->
    from_list([Value]).

-file("src/rx/eager.gleam", 17).
-spec from_result({ok, HWR} | {error, HWS}) -> eager(HWR, HWS).
from_result(Result) ->
    case Result of
        {ok, Value} ->
            'of'(Value);

        {error, Error} ->
            fail(Error)
    end.

-file("src/rx/eager.gleam", 28).
-spec empty() -> eager(any(), any()).
empty() ->
    from_list([]).

-file("src/rx/eager.gleam", 37).
?DOC(" Convert an eager sequence into the current actor-backed Observable API.\n").
-spec to_observable(eager(HXJ, HXK)) -> rx:observable(HXJ, HXK).
to_observable(Sequence) ->
    case Sequence of
        {eager, {ok, Values}} ->
            rx:from_list(Values);

        {eager, {error, Error}} ->
            rx:fail(Error)
    end.

-file("src/rx/eager.gleam", 46).
-spec to_result(eager(HXP, HXQ)) -> {ok, list(HXP)} | {error, HXQ}.
to_result(Sequence) ->
    {eager, Result} = Sequence,
    Result.

-file("src/rx/eager.gleam", 152).
-spec map_list_loop(list(IAR), fun((IAR) -> IAT), list(IAT)) -> list(IAT).
map_list_loop(Values, Mapper, Reversed) ->
    case Values of
        [] ->
            Reversed;

        [First | Rest] ->
            map_list_loop(Rest, Mapper, [Mapper(First) | Reversed])
    end.

-file("src/rx/eager.gleam", 148).
-spec map_list(list(IAN), fun((IAN) -> IAP)) -> list(IAP).
map_list(Values, Mapper) ->
    _pipe = map_list_loop(Values, Mapper, []),
    lists:reverse(_pipe).

-file("src/rx/eager.gleam", 51).
-spec map(eager(HXW, HXX), fun((HXW) -> HYA)) -> eager(HYA, HXX).
map(Sequence, Mapper) ->
    case Sequence of
        {eager, {ok, Values}} ->
            {eager, {ok, map_list(Values, Mapper)}};

        {eager, {error, Error}} ->
            {eager, {error, Error}}
    end.

-file("src/rx/eager.gleam", 58).
-spec map_error(eager(HYD, HYE), fun((HYE) -> HYH)) -> eager(HYD, HYH).
map_error(Sequence, Mapper) ->
    case Sequence of
        {eager, {ok, Values}} ->
            {eager, {ok, Values}};

        {eager, {error, Error}} ->
            {eager, {error, Mapper(Error)}}
    end.

-file("src/rx/eager.gleam", 170).
-spec filter_list_loop(list(IAZ), fun((IAZ) -> boolean()), list(IAZ)) -> list(IAZ).
filter_list_loop(Values, Predicate, Reversed) ->
    case Values of
        [] ->
            Reversed;

        [First | Rest] ->
            case Predicate(First) of
                true ->
                    filter_list_loop(Rest, Predicate, [First | Reversed]);

                false ->
                    filter_list_loop(Rest, Predicate, Reversed)
            end
    end.

-file("src/rx/eager.gleam", 163).
-spec filter_list(list(IAW), fun((IAW) -> boolean())) -> list(IAW).
filter_list(Values, Predicate) ->
    _pipe = filter_list_loop(Values, Predicate, []),
    lists:reverse(_pipe).

-file("src/rx/eager.gleam", 68).
-spec filter(eager(HYK, HYL), fun((HYK) -> boolean())) -> eager(HYK, HYL).
filter(Sequence, Predicate) ->
    case Sequence of
        {eager, {ok, Values}} ->
            {eager, {ok, filter_list(Values, Predicate)}};

        {eager, {error, Error}} ->
            {eager, {error, Error}}
    end.

-file("src/rx/eager.gleam", 78).
-spec tap(eager(HYQ, HYR), fun((HYQ) -> nil)) -> eager(HYQ, HYR).
tap(Sequence, Effect) ->
    map(
        Sequence,
        fun(Value) ->
            Effect(Value),
            Value
        end
    ).

-file("src/rx/eager.gleam", 189).
-spec take_list_loop(list(IBG), integer(), list(IBG)) -> list(IBG).
take_list_loop(Values, Count, Reversed) ->
    case Count =< 0 of
        true ->
            Reversed;

        false ->
            case Values of
                [] ->
                    Reversed;

                [First | Rest] ->
                    take_list_loop(Rest, Count - 1, [First | Reversed])
            end
    end.

-file("src/rx/eager.gleam", 185).
-spec take_list(list(IBD), integer()) -> list(IBD).
take_list(Values, Count) ->
    _pipe = take_list_loop(Values, Count, []),
    lists:reverse(_pipe).

-file("src/rx/eager.gleam", 88).
-spec take(eager(HYW, HYX), integer()) -> eager(HYW, HYX).
take(Sequence, Count) ->
    case Sequence of
        {eager, {ok, Values}} ->
            {eager, {ok, take_list(Values, Count)}};

        {eager, {error, Error}} ->
            {eager, {error, Error}}
    end.

-file("src/rx/eager.gleam", 204).
-spec skip_list(list(IBK), integer()) -> list(IBK).
skip_list(Values, Count) ->
    case Count =< 0 of
        true ->
            Values;

        false ->
            case Values of
                [] ->
                    [];

                [_ | Rest] ->
                    skip_list(Rest, Count - 1)
            end
    end.

-file("src/rx/eager.gleam", 95).
-spec skip(eager(HZC, HZD), integer()) -> eager(HZC, HZD).
skip(Sequence, Count) ->
    case Sequence of
        {eager, {ok, Values}} ->
            {eager, {ok, skip_list(Values, Count)}};

        {eager, {error, Error}} ->
            {eager, {error, Error}}
    end.

-file("src/rx/eager.gleam", 215).
-spec scan_list(list(IBN), IBP, fun((IBP, IBN) -> IBP), list(IBP)) -> list(IBP).
scan_list(Values, Accumulator, Reducer, Reversed) ->
    case Values of
        [] ->
            lists:reverse(Reversed);

        [First | Rest] ->
            Updated = Reducer(Accumulator, First),
            scan_list(Rest, Updated, Reducer, [Updated | Reversed])
    end.

-file("src/rx/eager.gleam", 102).
-spec scan(eager(HZI, HZJ), HZM, fun((HZM, HZI) -> HZM)) -> eager(HZM, HZJ).
scan(Sequence, Initial, Reducer) ->
    case Sequence of
        {eager, {ok, Values}} ->
            {eager, {ok, scan_list(Values, Initial, Reducer, [])}};

        {eager, {error, Error}} ->
            {eager, {error, Error}}
    end.

-file("src/rx/eager.gleam", 246).
-spec prepend_to_reversed(list(ICC), list(ICC)) -> list(ICC).
prepend_to_reversed(Values, Reversed) ->
    case Values of
        [] ->
            Reversed;

        [First | Rest] ->
            prepend_to_reversed(Rest, [First | Reversed])
    end.

-file("src/rx/eager.gleam", 230).
-spec flat_map_list(list(IBS), fun((IBS) -> eager(IBU, IBV)), list(IBU)) -> {ok,
        list(IBU)} |
    {error, IBV}.
flat_map_list(Values, Mapper, Reversed) ->
    case Values of
        [] ->
            {ok, lists:reverse(Reversed)};

        [First | Rest] ->
            case Mapper(First) of
                {eager, {error, Error}} ->
                    {error, Error};

                {eager, {ok, Mapped}} ->
                    flat_map_list(
                        Rest,
                        Mapper,
                        prepend_to_reversed(Mapped, Reversed)
                    )
            end
    end.

-file("src/rx/eager.gleam", 113).
-spec flat_map(eager(HZP, HZQ), fun((HZP) -> eager(HZT, HZQ))) -> eager(HZT, HZQ).
flat_map(Sequence, Mapper) ->
    case Sequence of
        {eager, {ok, Values}} ->
            {eager, flat_map_list(Values, Mapper, [])};

        {eager, {error, Error}} ->
            {eager, {error, Error}}
    end.

-file("src/rx/eager.gleam", 260).
-spec prepend_reversed(list(ICK), list(ICK)) -> list(ICK).
prepend_reversed(Reversed, Tail) ->
    case Reversed of
        [] ->
            Tail;

        [First | Rest] ->
            prepend_reversed(Rest, [First | Tail])
    end.

-file("src/rx/eager.gleam", 256).
-spec append_lists(list(ICG), list(ICG)) -> list(ICG).
append_lists(First, Second) ->
    prepend_reversed(lists:reverse(First), Second).

-file("src/rx/eager.gleam", 123).
-spec append(eager(HZY, HZZ), eager(HZY, HZZ)) -> eager(HZY, HZZ).
append(First, Second) ->
    case First of
        {eager, {error, Error}} ->
            {eager, {error, Error}};

        {eager, {ok, Left}} ->
            case Second of
                {eager, {error, Error@1}} ->
                    {eager, {error, Error@1}};

                {eager, {ok, Right}} ->
                    {eager, {ok, append_lists(Left, Right)}}
            end
    end.

-file("src/rx/eager.gleam", 267).
-spec fold_list(list(ICO), ICQ, fun((ICQ, ICO) -> ICQ)) -> ICQ.
fold_list(Values, Accumulator, Reducer) ->
    case Values of
        [] ->
            Accumulator;

        [First | Rest] ->
            fold_list(Rest, Reducer(Accumulator, First), Reducer)
    end.

-file("src/rx/eager.gleam", 137).
-spec fold(eager(IAG, IAH), IAK, fun((IAK, IAG) -> IAK)) -> {ok, IAK} |
    {error, IAH}.
fold(Sequence, Initial, Reducer) ->
    case Sequence of
        {eager, {error, Error}} ->
            {error, Error};

        {eager, {ok, Values}} ->
            {ok, fold_list(Values, Initial, Reducer)}
    end.
