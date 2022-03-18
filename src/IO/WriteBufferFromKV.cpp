#include <IO/WriteBufferFromKV.h>

namespace DB
{

WriteBufferFromKV::WriteBufferFromKV(std::unique_ptr<WriteBufferFromKVBase> impl_)
    : WriteBufferFromFileBase(0, nullptr, 0), impl(std::move(impl_))
{
    swap(*impl);
}

void WriteBufferFromKV::sync()
{
    impl->sync();
}

std::string WriteBufferFromKV::getFileName() const
{
    return impl->getKeyString();
}

void WriteBufferFromKV::finalize()
{
    if (finalized)
        return;
    next();
    impl->finalize();
    finalized = true;
}

/// |- next()
///    | --- nextImpl()
/// |- pos = working_buffer.begin()
void WriteBufferFromKV::nextImpl()
{
    swap(*impl);
    impl->next();
    swap(*impl);
}


};