#include "DiskLocalMemoryKV.h"

namespace DB
{

DiskLocalMemoryKV::DiskLocalMemoryKV(String name_, ContextPtr context_) : IDiskKV(name_, "IDiskKV"), context(context_)
{
}

bool DiskLocalMemoryKV::exists(const String &) const
{
    return true;
}

void DiskLocalMemoryKV::createFile(const String &)
{
}

size_t DiskLocalMemoryKV::getFileSize(const String &) const
{
    return 0;
}

void DiskLocalMemoryKV::moveFile(const String &, const String &)
{
}

void DiskLocalMemoryKV::replaceFile(const String &, const String &)
{
}

std::unique_ptr<ReadBufferFromFileBase>
DiskLocalMemoryKV::readFile(const String &, const ReadSettings &, std::optional<size_t>, std::optional<size_t>) const
{
    return nullptr;
}

std::unique_ptr<WriteBufferFromFileBase> DiskLocalMemoryKV::writeFile(const String &, size_t, WriteMode, const WriteSettings &)
{
    return nullptr;
}

void DiskLocalMemoryKV::removeFile(const String &)
{
}

void DiskLocalMemoryKV::removeFileIfExists(const String &)
{
}

};
