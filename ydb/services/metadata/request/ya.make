YQL_LIBRARY()

SRCS(
    request_actor.cpp
    request_actor_cb.cpp
    config.cpp  # gn: config
    common.cpp
)
# gn: config headers config.h
# gn: interface headers common.h request_actor.h request_actor_cb.h

PEERDIR(
    ydb/library/actors/core
    ydb/core/base
    ydb/core/grpc_services/local_rpc
    ydb/core/grpc_services/base
    ydb/core/grpc_services
    ydb/public/api/protos
)

END()
