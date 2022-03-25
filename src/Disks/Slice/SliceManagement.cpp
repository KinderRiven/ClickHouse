#include "SliceManagement.h"

namespace DB
{

SliceManagement & SliceManagement::instance()
{
    static SliceManagement manage;
    return manage;
}

std::shared_ptr<SliceManagement::SliceDownloadMetadata> SliceManagement::acquireDownloadSlice(const std::string & path)
{
    mutex.lock();

    auto it = slice_downloads.find(path);
    if (it != slice_downloads.end())
    {
        mutex.unlock();
        return it->second;
    }
    auto metadata = std::make_shared<SliceManagement::SliceDownloadMetadata>();
    slice_downloads[path] = metadata;
    mutex.unlock();
    return metadata;
}

};