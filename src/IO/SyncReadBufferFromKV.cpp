#include <IO/SyncReadBufferFromKV.h>

namespace DB
{

SyncReadBufferFromKV::SyncReadBufferFromKV(std::shared_ptr<KVBase> kv_, const String & key_)
    : ReadBufferFromKVBase(), kv_impl(std::move(kv_)), key(key_)
{
}

SyncReadBufferFromKV::~SyncReadBufferFromKV()
{
    if (copy_buf != nullptr)
        delete copy_buf;
}

bool SyncReadBufferFromKV::nextImpl()
{
    if (finalized)
        return false;

    /// read mark cache from key-value store
    bool hit = kv_impl->Get(key, value);

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

};