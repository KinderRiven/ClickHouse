#include <IO/ReadBufferFromKV.h>

using namespace DB;

ReadBufferFromKV::ReadBufferFromKV(SimpleKV * kv_, const String & key_) : ReadBufferFromFileBase(0, nullptr, 0), kv_store(kv_), key(key_)
{
}

 ReadBufferFromKV::~ReadBufferFromKV()
 {
     if (copy_buf != nullptr)
        delete copy_buf;
 }

bool ReadBufferFromKV::nextImpl()
{
    if (finalized)
        return false;

    /// read mark cache from key-value store
    bool hit = kv_store->get(key, value);

    if (!hit)
        return false;

    finalized = true;

    if (copy_buf != nullptr)
        delete copy_buf;

    copy_buf = new char[value.size()];
    memcpy(copy_buf, value.c_str(), value.size());
    set(copy_buf, value.size());
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