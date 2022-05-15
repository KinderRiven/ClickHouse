#include "DiskKVLocalMemory.h"

namespace DB
{

DiskKVLocalMemory::DiskKVLocalMemory(String name_, ContextPtr context_) : IDiskKV(name_, "DiskKVLocalMemory"), context(context_)
{
}

bool DiskKVLocalMemory::exists(const String &) const
{
    return true;
}

void DiskKVLocalMemory::createFile(const String &)
{
}

size_t DiskKVLocalMemory::getFileSize(const String &) const
{
    return 0;
}

void DiskKVLocalMemory::moveFile(const String &, const String &)
{
}

void DiskKVLocalMemory::replaceFile(const String &, const String &)
{
}

std::unique_ptr<ReadBufferFromFileBase>
DiskKVLocalMemory::readFile(const String &, const ReadSettings &, std::optional<size_t>, std::optional<size_t>) const
{
    return nullptr;
}

std::unique_ptr<WriteBufferFromFileBase> DiskKVLocalMemory::writeFile(const String &, size_t, WriteMode, const WriteSettings &)
{
    return nullptr;
}

void DiskKVLocalMemory::removeFile(const String &)
{
}

void DiskKVLocalMemory::removeFileIfExists(const String &)
{
}

};
