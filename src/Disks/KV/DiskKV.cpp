#include "DiskKV.h"
#include <IO/WriteBufferFromKV.h>

using namespace DB;

DiskKV::DiskKV()
{
    kv_impl = new SimpleKV();
}

bool DiskKV::exists(const String & path) const
{
    return kv_impl->exists(path);
}

size_t DiskKV::getFileSize(const String & path) const
{
    String value;
    kv_impl->get(path, value);
    return value.size();
}

void DiskKV::createFile(const String & path)
{
    String value = ""; /// value with empty
    if (!exists(path))
    {
        kv_impl->put(path, value);
    }
}

std::unique_ptr<ReadBufferFromFileBase> DiskKV::readFile(const String & path, const ReadSettings &, std::optional<size_t> size) const
{
    return nullptr;
}

std::unique_ptr<WriteBufferFromFileBase> DiskKV::writeFile(const String &, size_t, WriteMode)
{
    auto kv_buffer = std::make_unique<WriteBufferFromKV>(kv_impl, path, size);
    return std::make_unique<WriteBufferFromFileDecorator(kv_buffer)>;
}

void DiskKV::removeFile(const String & path)
{
    kv_impl->erase(path);
}

void DiskKV::removeFileIfExists(const String & path)
{
    kv_impl->erase(path);
}

void DiskKV::shutdown()
{
    /// TODO shutdown
}

void DiskKV::startup()
{
    /// TODO startup
}