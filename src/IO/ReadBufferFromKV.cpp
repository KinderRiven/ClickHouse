#include <IO/ReadBufferFromKV.h>

using namespace DB;

ReadBufferFromKV::ReadBufferFromKV(SimpleKV * kv_, const String & key_) : ReadBufferFromFileBase(0, nullptr, 0), kv_store(kv_), key(key_)
{
}

bool ReadBufferFromKV::nextImpl()
{
    if (finalized)
        return false;

    /// std::string(working_buffer.begin(), pos)
    value = kv_store->get[key];
    finalized = true;

    /// set ReadBuffer
    set(value.c_str(), value.size());
    return true;
}

off_t ReadBufferFromKV::getPosition()
{
    return 0;
}

off_t ReadBufferFromKV::seek(off_t, int)
{
    return 0;
}