#pragma once

#include <atomic>
#include <list>
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

class SliceReadBuffer;

enum SliceDownloadStatus
{
    SLICE_NONE,
    SLICE_PREFETCH,
    SLICE_DOWNLOADING,
    SLICE_DOWNLOADED,
    SLICE_ERROR,
    SLICE_DELETE,
};

struct SliceDownloadMetadata
{
public:
    /// Thread waits on this condition if download process is in progress.
    std::mutex mutex;
    std::atomic<int> access = 0;
    size_t size = 0;
    std::atomic<SliceDownloadStatus> status = SLICE_NONE;

public:
    SliceDownloadMetadata(size_t size_) : size(size_) { }

    void Access() { access++; }

    void setDownloaded() { status = SliceDownloadStatus::SLICE_DOWNLOADED; }

    void setPrefetch() { status = SliceDownloadStatus::SLICE_PREFETCH; }

    void setDownloading() { status = SliceDownloadStatus::SLICE_DOWNLOADING; }

    void setDelete() { status = SliceDownloadStatus::SLICE_DELETE; }

    bool isLoading()
    {
        if ((status == SliceDownloadStatus::SLICE_DOWNLOADING) || (status == SliceDownloadStatus::SLICE_PREFETCH))
        {
            return true;
        }
        return false;
    }

    bool isDownloaded() { return status == SliceDownloadStatus::SLICE_DOWNLOADED ? true : false; }

    bool isDelete() { return status == SliceDownloadStatus::SLICE_DELETE ? true : false; }

    bool canDownload() { return status == SliceDownloadStatus::SLICE_NONE ? true : false; }

    bool tryLock() { return mutex.try_lock(); }

    void Lock() { mutex.lock(); }

    void Unlock() { mutex.unlock(); }
};


class SliceManagement
{
public:
    using SlicePtr = std::shared_ptr<SliceDownloadMetadata>;
    using SliceList = std::list<std::pair<const String, SlicePtr>>;

public:
    static SliceManagement & instance();

    void initlizate(ContextPtr context_);

    void setupLocalCacheDisk(std::shared_ptr<IDisk> local_disk);

    void setupRemoteCacheDisk(std::shared_ptr<IDisk> remote_disk);

    std::unique_ptr<WriteBufferFromFileBase>
    createRemoteFileToUpload(const String & key, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE, WriteMode mode = WriteMode::Rewrite);

    std::unique_ptr<ReadBufferFromFileBase>
    tryToReadSliceFromRemote(const String & key, const ReadSettings & settings = ReadSettings{}, std::optional<size_t> size = {});

    SlicePtr acquireDownloadSlice(const std::string & path);

    SlicePtr tryToAddBackgroundDownloadTask(const String & path, int slice_id);

    void tryToAddBackgroundCleanupTask();

    void cleanupMainList();

private:
    /// SliceManagement() = default;
    SliceManagement() { total_space_size = 512UL * 1024 * 1024; };

    void traverseToLoad(const String & path);

private:
    /// Contains information about currently running file downloads to cache.
    mutable std::unordered_map<std::string, SliceList::iterator> slice_downloads;

    /// Protects concurrent downloading files to cache.
    mutable std::mutex mutex;

    std::shared_ptr<IDisk> local_disk;

    std::shared_ptr<IDisk> remote_disk;

    Poco::Logger * log = &Poco::Logger::get("[SliceManagement]");

    std::shared_ptr<CacheJobsAssignee> background_downloads_assignee = nullptr;

    std::shared_ptr<CacheJobsAssignee> background_cleanup_assignee = nullptr;

    bool is_cleanup = false;

    size_t total_space_size = 0;

    size_t usage_space_size = 0;

    ContextPtr context = nullptr;

    bool hasInit = false;

    SliceList main_list;
};
};