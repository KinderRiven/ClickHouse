#pragma once

#include <atomic>
#include <list>
#include <mutex>
#include <string>
#include <tuple>
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


struct SlicePrefetchTask
{
public:
    std::shared_ptr<IDisk> local_disk;
    std::shared_ptr<IDisk> remote_disk;
    ReadSettings read_settings;
    const String filename;
    std::vector<std::tuple<int, size_t, size_t>> vec_slice; /// <slice_id, offset, size>

public:
    SlicePrefetchTask(
        std::shared_ptr<IDisk> local_disk_, std::shared_ptr<IDisk> remote_disk_, ReadSettings & read_settings_, const String filename_)
        : local_disk(local_disk_), remote_disk(remote_disk_), read_settings(read_settings_), filename(filename_)
    {
    }

    void Add(int slice_id, size_t offset, size_t size) { vec_slice.push_back(std::make_tuple(slice_id, offset, size)); }

    size_t Count() { return vec_slice.size(); }
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

    void tryToAddBackgroundPrefetchTask(std::shared_ptr<SlicePrefetchTask> task);

    void tryToAddBackgroundCleanupTask();

    void cleanupMainList();

    void handlePrefetch();

private:
    /// SliceManagement() = default;
    SliceManagement() { total_space_size = 512UL * 1024 * 1024; };

    void traverseToLoad(const String & path);

private:
    /// Contains information about currently running file downloads to cache.
    mutable std::unordered_map<std::string, SliceList::iterator> slice_downloads;

    /// Protects concurrent downloading files to cache.
    mutable std::mutex mutex;

    /// local cache disk
    std::shared_ptr<IDisk> local_disk;

    /// remote cache disk
    std::shared_ptr<IDisk> remote_disk;

    /// log
    Poco::Logger * log = &Poco::Logger::get("[SliceManagement]");

    /// cleanup
    std::shared_ptr<CacheJobsAssignee> background_cleanup_assignee = nullptr;

    SliceList main_list;

    bool is_cleanup = false;

    /// prefetch
    std::shared_ptr<CacheJobsAssignee> background_prefetch_assignee = nullptr;

    std::mutex prefetch_mutex;

    std::queue<std::shared_ptr<SlicePrefetchTask>> prefetch_queue;

    bool is_prefetch = false;

    /// total slice cache can use space size
    size_t total_space_size = 0;

    /// usage cspace size
    size_t usage_space_size = 0;

    ContextPtr context = nullptr;

    bool hasInit = false;
};
};