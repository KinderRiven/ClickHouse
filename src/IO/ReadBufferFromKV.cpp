#include <IO/ReadBufferFromKV.h>

using namespace DB;

ReadBufferFromKV::ReadBufferFromKV(SimpleKV * kv_, String & key_) : ReadBufferFromFileBase(0, nullptr, 0), kv_store(kv_), key(key_)
{
}

bool ReadBufferFromKV::nextImpl()
{
    /// TODO update value
    /// std::string(working_buffer.begin(), pos)
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