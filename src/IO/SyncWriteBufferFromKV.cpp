#include <IO/SyncWriteBufferFromKV.h>

using namespace DB;

SyncWriteBufferFromKV::SyncWriteBufferFromKV(std::shared_ptr<KVBase> kv_, const String & key_, size_t value_length_)
    : WriteBufferFromKVBase(value_length_), kv_impl(std::move(kv_)), key(key_)
{
}

SyncWriteBufferFromKV::~SyncWriteBufferFromKV() = default;

void SyncWriteBufferFromKV::sync()
{
    next();
}

/// |- next()
///    | --- nextImpl()
/// |- pos = working_buffer.begin()
void SyncWriteBufferFromKV::nextImpl()
{
    /// update put value
    size_t sub_string_size = static_cast<size_t>(pos - working_buffer.begin());
    value += String(working_buffer.begin(), sub_string_size);
}

void SyncWriteBufferFromKV::finalize()
{
    next(); /// update value
    kv_impl->Put(key, value);
}