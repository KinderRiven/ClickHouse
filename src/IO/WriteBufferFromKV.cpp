#include <IO/WriteBufferFromKV.h>

using namespace DB;

WriteBufferFromKV::WriteBufferFromKV(SimpleKV * kv_, String & key_, size_t value_length_)
    : BufferWithOwnMemory<WriteBuffer>(value_length_, nullptr, 0), kv_store(kv_), key(key_), value_length(value_length_)
{
}

void WriteBufferFromKV::nextImpl()
{
    /// TODO update value
}

void WriteBufferFromKV::finalize()
{
    if (finalized)
        return;

    next();

    kv_store->put(key, value);
    finalized = true;
}