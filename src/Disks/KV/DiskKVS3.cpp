#include "DiskKVS3.h"

namespace DB
{

DiskKVS3::DiskKVS3(String name_, ContextPtr context_) : IDiskKV(name_, "DiskKVS3"), context(context_)
{
}

bool DiskKVS3::exists(const String &) const
{
    return true;
}

void DiskKVS3::createFile(const String &)
{
}

size_t DiskKVS3::getFileSize(const String &) const
{
    return 0;
}

void DiskKVS3::moveFile(const String &, const String &)
{
}

void DiskKVS3::replaceFile(const String &, const String &)
{
}

std::unique_ptr<ReadBufferFromFileBase>
DiskKVS3::readFile(const String &, const ReadSettings &, std::optional<size_t>, std::optional<size_t>) const
{
    return nullptr;
}

std::unique_ptr<WriteBufferFromFileBase> DiskKVS3::writeFile(const String &, size_t, WriteMode, const WriteSettings &)
{
    return nullptr;
}

void DiskKVS3::removeFile(const String &)
{
}

void DiskKVS3::removeFileIfExists(const String &)
{
}

};
