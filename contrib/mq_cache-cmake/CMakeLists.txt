set(MQCACHE_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/mq_cache")

set(MQCACHE_INC
    ${MQCACHE_SOURCE_DIR}/src
)

set(MQCACHE_SRC
    ${MQCACHE_SOURCE_DIR}/src/Rpc/Protoc/ClientService.protoc.grpc.pb.cc
    ${MQCACHE_SOURCE_DIR}/src/Rpc/Protoc/ClientService.protoc.pb.cc
    ${MQCACHE_SOURCE_DIR}/src/Rpc/GRpc/GRpcClientCaller.cpp
    ${MQCACHE_SOURCE_DIR}/src/Connector/Connector.cpp
)

add_library(_mq_cache ${MQCACHE_SRC})
target_include_directories(_mq_cache PUBLIC ${MQCACHE_INC})
target_link_libraries(_mq_cache PUBLIC ch_contrib::grpc ch_contrib::json ch_contrib::spdlog)
add_library(ch_contrib::mq_cache ALIAS _mq_cache)
