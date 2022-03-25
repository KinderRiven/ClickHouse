#pragma once

#include <mutex>
#include <string>
#include <unordered_map>
#include <base/logger_useful.h>

namespace DB
{

class SliceManagement
{
public:
    enum SliceDownloadStatus
    {
        NONE,
        DOWNLOADING,
        DOWNLOADED,
        ERROR
    };

    struct SliceDownloadMetadata
    {
        /// Thread waits on this condition if download process is in progress.
        std::mutex mutex;
        SliceDownloadStatus status = NONE;
    };

public:
    static SliceManagement & instance();

    std::shared_ptr<SliceManagement::SliceDownloadMetadata> acquireDownloadSlice(const std::string & path);

private:
    SliceManagement() = default;

private:
    /// Contains information about currently running file downloads to cache.
    mutable std::unordered_map<std::string, std::shared_ptr<SliceDownloadMetadata>> slice_downloads;

    /// Protects concurrent downloading files to cache.
    mutable std::mutex mutex;
};
};