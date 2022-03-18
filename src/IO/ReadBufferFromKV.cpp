#include <IO/ReadBufferFromKV.h>

namespace DB
{

ReadBufferFromKV::ReadBufferFromKV(std::unique_ptr<ReadBufferFromKVBase> impl_) : ReadBufferFromFileBase(), kv_impl(std::move(impl_))
{
    swap(*kv_impl);
}

ReadBufferFromKV::~ReadBufferFromKV()
{
}

bool ReadBufferFromKV::nextImpl()
{
    swap(*kv_impl);
    auto result = kv_impl->next();
    swap(*kv_impl);
    return result;
}

off_t ReadBufferFromKV::getPosition()
{
    return 0;
}

off_t ReadBufferFromKV::seek(off_t, int)
{
    return 0;
}

};
