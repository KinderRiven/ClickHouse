#include "DiskKV.h"

using namespace DB;

DiskKV::DiskKV()
{
    kv_impl = make_shared<SimpleKV()>;
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

void DiskKV::listFiles(const String & path, std::vector<String> & file_names)
{
}

std::unique_ptr<ReadBufferFromFileBase>
DiskKV::readFile(const String & path, const ReadSettings & settings = ReadSettings{}, std::optional<size_t> size = {}) const
{
    return nullptr;
}

std::unique_ptr<WriteBufferFromFileBase>
DiskKV::writeFile(const String & path, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE, WriteMode mode = WriteMode::Rewrite)
{
    return nullptr;
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