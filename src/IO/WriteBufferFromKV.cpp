#include <IO/WriteBufferFromKV.h>

using namespace DB;

WriteBufferFromKV::WriteBufferFromKV(SimpleKV * kv_, String & key_, size_t value_length_)
    : WriteBufferFromFileBase(value_length_, nullptr, 0), kv_store(kv_), key(key_), value_length(value_length_)
{
}

void WriteBufferFromKV::sync()
{
    next();
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