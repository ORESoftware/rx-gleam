-module(rx_gleam_test).
-compile([no_auto_import, nowarn_unused_vars, nowarn_unused_function, nowarn_nomatch, inline]).
-define(FILEPATH, "test/rx_gleam_test.gleam").
-export([main/0]).

-file("test/rx_gleam_test.gleam", 3).
-spec main() -> nil.
main() ->
    gleeunit:main().
