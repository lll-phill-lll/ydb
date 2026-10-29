YQL_LIBRARY()

SRCS(
    result.cpp
    events.cpp
)
# gn: interface headers events.h result.h

PEERDIR(
    ydb/core/kqp/query_data
    ydb/core/kqp/common/simple
    yql/essentials/public/issue
)

END()
