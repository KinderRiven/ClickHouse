#include <IO/ReadBufferFromKV.h>

using namespace DB;

ReadBufferFromKV::ReadBufferFromKV(SimpleKV * kv_, String & key_, size_t value_length_)
    : ReadBufferFromFileBase(value_length_, nullptr, 0), kv_store(kv_), key(key_), value_length(value_length_)
{
}


bool RedaBufferFromKV::nextImpl()
{
    /// TODO update value
    return false;
}