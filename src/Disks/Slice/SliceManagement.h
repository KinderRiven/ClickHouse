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

    String query_id;

    std::atomic<int> access = 0;

    std::atomic<int> num_ref = 0;

    size_t size = 0;

    std::atomic<SliceDownloadStatus> status = SLICE_NONE;

public:
    SliceDownloadMetadata(String & query_id_, size_t size_) : query_id(query_id_), size(size_) { }

    void setQueryId(const String & query_id_) { query_id = query_id_; }

    void Access() { access++; }

    void SubRef() { num_ref++; }

    void DecRef() { num_ref--; }

    int NumRef() const { return num_ref; }

    void setDownloaded() { status = SliceDownloadStatus::SLICE_DOWNLOADED; }

    void setPrefetch() { status = SliceDownloadStatus::SLICE_PREFETCH; }

    void setDownloading() { status = SliceDownloadStatus::SLICE_DOWNLOADING; }

    void setDelete() { status = SliceDownloadStatus::SLICE_DELETE; }

    bool isLoading() const
    {
        if ((status == SliceDownloadStatus::SLICE_DOWNLOADING) || (status == SliceDownloadStatus::SLICE_PREFETCH))
        {
            return true;
        }
        return false;
    }

    bool isDownloaded() const { return status == SliceDownloadStatus::SLICE_DOWNLOADED ? true : false; }

    bool isDelete() const { return status == SliceDownloadStatus::SLICE_DELETE ? true : false; }

    bool canDownload() const { return status == SliceDownloadStatus::SLICE_NONE ? true : false; }

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
    struct SliceListMetadata
    {
    public:
        const String list_name;

        int num_ref = 0;

        size_t usage_space_bytes = 0;

        size_t total_space_bytes = 0;

        std::shared_ptr<SliceList> list;

    public:
        SliceListMetadata(const String & list_name_, size_t total_space_bytes_)
            : list_name(list_name_), total_space_bytes(total_space_bytes_)
        {
            list = std::make_shared<SliceList>();
        }

        void SubRef() { num_ref++; }

        void DecRef() { num_ref--; }

        int NumRef() const { return num_ref; }

        void PushBack(const String & path, SlicePtr slice)
        {
            usage_space_bytes += slice->size;
            list->push_back(std::make_pair(path, slice));
        }

        void Erase(SliceList::iterator & it)
        {
            usage_space_bytes -= (it->second->size);
            list->erase(it);
        }

        void PopFront()
        {
            usage_space_bytes -= (list->front().second->size);
            list->pop_front();
        }

        SliceList::iterator LastIterator() const
        {
            auto end_iter = list->end();
            std::advance(end_iter, -1);
            return end_iter;
        }

        SliceList::iterator FirstIterator() const { return list->begin(); }

        size_t SliceCount() const { return list->size(); }

        size_t UsageBytes() const { return usage_space_bytes; }

        size_t UsageMB() const { return usage_space_bytes / (1024 * 1024); }

        size_t TotalBytes() const { return total_space_bytes; }

        size_t TotalMB() const { return total_space_bytes / (1024 * 1024); }

        const String & ListName() const { return list_name; }

        std::shared_ptr<SliceList> List() const { return list; }

        bool isFull() { return usage_space_bytes >= total_space_bytes ? true : false; }
    };

    /// record a query cache result.
    struct QueryStats
    {
    public:
        size_t cache_hit = 0;

        size_t cache_miss = 0;

    public:
        double HitRatio() const
        {
            size_t total_access_count = cache_hit + cache_miss;
            if (!total_access_count)
            {
                return 0.0;
            }
            else
            {
                return ((1.0 * cache_hit) / total_access_count);
            }
        }

        void Hit() { cache_hit++; }

        void Miss() { cache_miss++; }
    };

    /// cache stat
    struct CacheStats
    {
    public:
        std::unordered_map<String, std::shared_ptr<QueryStats>> stats;

        size_t prefetch_count = 0;

        size_t prefetch_hit = 0;

    public:
        void addQueryHit(const String & query_id)
        {
            auto iter = stats.find(query_id);
            if (iter == stats.end())
            {
                stats[query_id] = std::make_shared<QueryStats>();
                iter = stats.find(query_id);
            }
            iter->second->Hit();
        }

        void addQueryMiss(const String & query_id)
        {
            auto iter = stats.find(query_id);
            if (iter == stats.end())
            {
                stats[query_id] = std::make_shared<QueryStats>();
                iter = stats.find(query_id);
            }
            iter->second->Miss();
        }

        double getQueryHitRatio(const String & query_id) const
        {
            auto iter = stats.find(query_id);
            if (iter == stats.end())
            {
                return 0.0;
            }
            return iter->second->HitRatio();
        }

        void addPrefetch(size_t num) { prefetch_count += num; }

        void addPrefetchHit() { prefetch_hit++; }

        double getPrefetchHitRatio()
        {
            if (prefetch_count == 0)
            {
                return 0.0;
            }
            else
            {
                return ((1.0 * prefetch_hit) / prefetch_count);
            }
        }
    };

public:
    static SliceManagement & instance();

    void initlizate(ContextPtr context_);

    void setupLocalCacheDisk(std::shared_ptr<IDisk> local_disk);

    void setupRemoteCacheDisk(std::shared_ptr<IDisk> remote_disk);

    std::unique_ptr<WriteBufferFromFileBase>
    createRemoteFileToUpload(const String & key, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE, WriteMode mode = WriteMode::Rewrite);

    std::unique_ptr<ReadBufferFromFileBase>
    tryToReadSliceFromRemote(const String & key, const ReadSettings & settings = ReadSettings{}, std::optional<size_t> size = {});

    SlicePtr acquireDownloadSlice(String & query_id, const std::string & path);

    void tryToAddBackgroundPrefetchTask(std::shared_ptr<SlicePrefetchTask> task);

    void tryToAddBackgroundCleanupTask();

    void cleanupMainList();

    void handlePrefetch();

    void setQueryContext(String & query_id);

    void freeQueryContext(String & query_id);

private:
    /// SliceManagement() = default;
    SliceManagement() { total_space_size = 512UL * 1024 * 1024; };

    void traverseToLoad(const String & path);

    void listMove(std::shared_ptr<SliceListMetadata> from, std::shared_ptr<SliceListMetadata> to);

    void tryToFreeListSpace(std::shared_ptr<SliceListMetadata> list);

private:
    /// Contains information about currently running file downloads to cache.
    mutable std::unordered_map<std::string, SliceList::iterator> slice_downloads;

    /// Protects concurrent downloading files to cache.
    mutable std::mutex mutex;

    /// list map
    std::unordered_map<String, std::shared_ptr<SliceListMetadata>> list_map;

    /// local cache disk
    std::shared_ptr<IDisk> local_disk;

    /// remote cache disk
    std::shared_ptr<IDisk> remote_disk;

    /// log
    Poco::Logger * log = &Poco::Logger::get("[SliceManagement]");

    /// cleanup
    std::shared_ptr<CacheJobsAssignee> background_cleanup_assignee = nullptr;

    bool is_cleanup = false;

    /// prefetch
    std::shared_ptr<CacheJobsAssignee> background_prefetch_assignee = nullptr;

    std::mutex prefetch_mutex;

    std::queue<std::shared_ptr<SlicePrefetchTask>> prefetch_queue;

    bool is_prefetch = false;

    /// total slice cache can use space size
    size_t total_space_size = 0;

    ContextPtr context = nullptr;

    bool hasInit = false;

    CacheStats stats;
};
};