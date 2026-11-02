YQL_LIBRARY()

SRCS(
    cpu_load_actors.cpp  # gn: cpu_load_actors
    pool_handlers_actors.cpp
    resource_pool_tracker.cpp
    scheme_actors.cpp  # gn: scheme_actors
    workload_manager_state_actor.cpp
)

PEERDIR(
    ydb/core/tx/tx_proxy
    ydb/services/workload_manager/common
    ydb/services/workload_manager/tables
)

END()

RECURSE_FOR_TESTS(
    ut
)
