-record(state, {
    concurrency :: integer(),
    order :: rx@flow_model:order(),
    next_sequence :: integer(),
    next_emit :: integer(),
    pending :: list(integer()),
    active :: list(integer()),
    completed :: list(integer()),
    emitted :: list(integer()),
    input_done :: boolean(),
    status :: rx@flow_model:status()
}).
