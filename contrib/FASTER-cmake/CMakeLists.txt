option(ENABLE_FASTER "Enable FASTER support" ${ENABLE_LIBRARIES})
if (NOT ENABLE_FASTER)
    message (STATUS "FASTER disabled")
    return()
endif()

set(FASTER_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/FASTER/cc/src")

# Build the FASTER library.
set (FASTER_HEADERS
    ${FASTER_SOURCE_DIR}/core/address.h
    ${FASTER_SOURCE_DIR}/core/alloc.h
    ${FASTER_SOURCE_DIR}/core/async.h
    ${FASTER_SOURCE_DIR}/core/async_result_types.h
    ${FASTER_SOURCE_DIR}/core/auto_ptr.h
    ${FASTER_SOURCE_DIR}/core/checkpoint_locks.h
    ${FASTER_SOURCE_DIR}/core/checkpoint_state.h
    ${FASTER_SOURCE_DIR}/core/constants.h
    ${FASTER_SOURCE_DIR}/core/faster.h
    ${FASTER_SOURCE_DIR}/core/gc_state.h
    ${FASTER_SOURCE_DIR}/core/grow_state.h
    ${FASTER_SOURCE_DIR}/core/guid.h
    ${FASTER_SOURCE_DIR}/core/hash_bucket.h
    ${FASTER_SOURCE_DIR}/core/hash_table.h
    ${FASTER_SOURCE_DIR}/core/internal_contexts.h
    ${FASTER_SOURCE_DIR}/core/key_hash.h
    ${FASTER_SOURCE_DIR}/core/light_epoch.h
    ${FASTER_SOURCE_DIR}/core/lss_allocator.h
    ${FASTER_SOURCE_DIR}/core/malloc_fixed_page_size.h
    ${FASTER_SOURCE_DIR}/core/native_buffer_pool.h
    ${FASTER_SOURCE_DIR}/core/persistent_memory_malloc.h
    ${FASTER_SOURCE_DIR}/core/phase.h
    ${FASTER_SOURCE_DIR}/core/record.h
    ${FASTER_SOURCE_DIR}/core/recovery_status.h
    ${FASTER_SOURCE_DIR}/core/state_transitions.h
    ${FASTER_SOURCE_DIR}/core/status.h
    ${FASTER_SOURCE_DIR}/core/thread.h
    ${FASTER_SOURCE_DIR}/core/utility.h
    ${FASTER_SOURCE_DIR}/device/file_system_disk.h
    ${FASTER_SOURCE_DIR}/device/null_disk.h
    ${FASTER_SOURCE_DIR}/environment/file.h
    ${FASTER_SOURCE_DIR}/environment/file_common.h
)

set (FASTER_HEADERS ${FASTER_HEADERS}
    ${FASTER_SOURCE_DIR}/environment/file_linux.h
)

set (FASTER_SOURCES
    ${FASTER_SOURCE_DIR}/core/address.cc
    ${FASTER_SOURCE_DIR}/core/lss_allocator.cc
    ${FASTER_SOURCE_DIR}/core/thread.cc
)

set (FASTER_SOURCES ${FASTER_SOURCES}
    ${FASTER_SOURCE_DIR}/environment/file_linux.cc
)

add_library(_faster ${FASTER_SOURCES} ${FASTER_HEADERS})
add_library(ch_contrib::FASTER ALIAS _faster)
target_include_directories(_faster SYSTEM BEFORE INTERFACE "${FASTER_HEADERS}")
