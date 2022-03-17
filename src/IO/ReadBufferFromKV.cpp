#include <IO/ReadBufferFromKV.h>

using namespace DB;

ReadBufferFromKV::ReadBufferFromKV(SimpleKV * kv_, String & key_, size_t value_length_)
    : ReadBufferFromFileBase(value_length_, nullptr, 0), kv_store(kv_), key(key_), value_length(value_length_)
{
}


bool ReadBufferFromKV::nextImpl()
{
    /// TODO update value
    return false;
}

off_t ReadBufferFromKV::getPosition()
{
    return 0;
}

off_t ReadBufferFromKV::seek(off_t, int)
{
    return 0;
}