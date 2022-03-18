#include "DiskKV.h"
#include <IO/ReadBufferFromKV.h>
#include <IO/SyncReadBufferFromKV.h>
#include <IO/SyncWriteBufferFromKV.h>
#include <IO/WriteBufferFromKV.h>

namespace DB
{

DiskKV::DiskKV(std::shared_ptr<KVBase> kv) : kv_impl(std::move(kv))
{
}

bool DiskKV::exists(const String & path) const
{
    return kv_impl->Exists(path);
}

size_t DiskKV::getFileSize(const String & path) const
{
    String value;
    kv_impl->Get(path, value);
    return value.size();
}

void DiskKV::createFile(const String & path)
{
    String value = "";
    if (!exists(path))
    {
        kv_impl->Put(path, value);
    }
}

std::unique_ptr<ReadBufferFromFileBase> DiskKV::readFile(const String & path, const ReadSettings &, std::optional<size_t>) const
{
    auto impl = std::make_unique<SyncReadBufferFromKV>(kv_impl, path);
    return std::make_unique<ReadBufferFromKV>(std::move(impl));
}

std::unique_ptr<WriteBufferFromFileBase> DiskKV::writeFile(const String & path, size_t size, WriteMode)
{
    auto impl = std::make_unique<SyncWriteBufferFromKV>(kv_impl, path, size);
    return std::make_unique<WriteBufferFromKV>(std::move(impl));
}

void DiskKV::removeFile(const String & path)
{
    kv_impl->Delete(path);
}

void DiskKV::removeFileIfExists(const String & path)
{
    kv_impl->Delete(path);
}

void DiskKV::shutdown()
{
    /// TODO shutdown
}

void DiskKV::startup()
{
    /// TODO startup
}

};