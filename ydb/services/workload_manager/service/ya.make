LIBRARY()

SRCS(
    service.cpp
)
# gn: ids headers service_id.h

PEERDIR(
    ydb/core/base
    ydb/core/cms/console
    ydb/core/kqp/common
    ydb/core/kqp/common/events
    ydb/core/kqp/runtime
    ydb/core/mind  # gn: :tenant_node_enumeration
    ydb/core/protos
    ydb/core/resource_pools
    ydb/library/aclib
    ydb/library/actors/core
    ydb/library/actors/interconnect
    ydb/services/metadata
    ydb/services/workload_manager
    ydb/services/workload_manager/actors  # gn: :cpu_load_actors
    ydb/services/workload_manager/common
    ydb/services/workload_manager/metadata_subscription/resource_pool_classifier
    ydb/services/workload_manager/tables
)

YQL_LAST_ABI_VERSION()

END()

