add_library(_json INTERFACE)
target_include_directories(_json SYSTEM BEFORE INTERFACE "${ClickHouse_SOURCE_DIR}/contrib/json/include")
add_library(ch_contrib::json ALIAS _json)
