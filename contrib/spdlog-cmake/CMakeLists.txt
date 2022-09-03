add_library(_spdlog INTERFACE)
target_include_directories(_spdlog SYSTEM BEFORE INTERFACE "${ClickHouse_SOURCE_DIR}/contrib/spdlog/include")
add_library(ch_contrib::spdlog ALIAS _spdlog)
