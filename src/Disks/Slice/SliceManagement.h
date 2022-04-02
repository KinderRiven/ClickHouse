#pragma once

#include <mutex>
#include <string>
#include <unordered_map>
#include <Disks/IDisk.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/Cache/CacheJobsAssignee.h>
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

    void initlizate(ContextPtr context_);

    void setupRemoteCacheDisk(std::shared_ptr<IDisk> remote_disk);

    std::unique_ptr<WriteBufferFromFileBase>
    createRemoteFileToUpload(const String & key, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE, WriteMode mode = WriteMode::Rewrite);

    std::unique_ptr<ReadBufferFromFileBase>
    tryToReadSliceFromRemote(const String & key, const ReadSettings & settings = ReadSettings{}, std::optional<size_t> size = {});

    std::shared_ptr<SliceManagement::SliceDownloadMetadata> acquireDownloadSlice(const std::string & path);

private:
    /// SliceManagement() = default;
    SliceManagement() = default;

private:
    /// Contains information about currently running file downloads to cache.
    mutable std::unordered_map<std::string, std::shared_ptr<SliceDownloadMetadata>> slice_downloads;

    /// Protects concurrent downloading files to cache.
    mutable std::mutex mutex;

    std::shared_ptr<IDisk> remote_disk;

    Poco::Logger * log = &Poco::Logger::get("[SliceManagement]");

    std::shared_ptr<CacheJobsAssignee> background_downloads_assignee = nullptr;

    ContextPtr context = nullptr;

    bool hasInit = false;
};
};