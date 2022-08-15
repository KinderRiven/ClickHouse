# lz4 is the main compression method, cannot be disabled.

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/MQCacheConnector")
set (SRCS
    "${LIBRARY_DIR}/src/HelloConnector.cpp"
)

add_library (_mqcache ${SRCS})
add_library (ch_contrib::mqcache ALIAS _mqcache)
target_include_directories(_mqcache PUBLIC "${LIBRARY_DIR}/include")
