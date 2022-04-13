#pragma once

#include <atomic>
#include <ctime>
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

/// #define SLICE_DEBUG
/// #define SLICE_WATCH
#define USE_SLICE_PREFETCH

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

        size_t NeedToFreeBytes() const
        {
            if (usage_space_bytes < total_space_bytes)
            {
                return 0;
            }
            else
            {
                double need_to_free = 1.0 * usage_space_bytes - 0.75 * total_space_bytes;
                return static_cast<size_t>(need_to_free);
            }
        }

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

    /// record a slice cache result
    struct SliceStats
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
        std::unordered_map<String, std::shared_ptr<QueryStats>> query_stats;

        std::unordered_map<String, std::shared_ptr<SliceStats>> slice_stats;

        size_t prefetch_count = 0;

        size_t prefetch_hit = 0;

    public:
        void addQueryHit(const String & query_id)
        {
            auto iter = query_stats.find(query_id);
            if (iter == query_stats.end())
            {
                query_stats[query_id] = std::make_shared<QueryStats>();
                iter = query_stats.find(query_id);
            }
            iter->second->Hit();
        }

        void addQueryMiss(const String & query_id)
        {
            auto iter = query_stats.find(query_id);
            if (iter == query_stats.end())
            {
                query_stats[query_id] = std::make_shared<QueryStats>();
                iter = query_stats.find(query_id);
            }
            iter->second->Miss();
        }

        double getQueryHitRatio(const String & query_id) const
        {
            auto iter = query_stats.find(query_id);
            if (iter == query_stats.end())
            {
                return 0.0;
            }
            return iter->second->HitRatio();
        }

        void addSliceHit(const String & slice)
        {
            auto iter = slice_stats.find(slice);
            if (iter == slice_stats.end())
            {
                slice_stats[slice] = std::make_shared<SliceStats>();
                iter = slice_stats.find(slice);
            }
            iter->second->Hit();
        }

        void addSliceMiss(const String & slice)
        {
            auto iter = slice_stats.find(slice);
            if (iter == slice_stats.end())
            {
                slice_stats[slice] = std::make_shared<SliceStats>();
                iter = slice_stats.find(slice);
            }
            iter->second->Miss();
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

        void generateTraceToDisk(const String & path, std::shared_ptr<IDisk> disk)
        {
            auto writer = disk->writeFile(path, 1048576UL, WriteMode::Rewrite);
            for (auto & stat : slice_stats)
            {
                /// -----------------------------------------------------
                /// | entry_size | slice_name |  hit count | miss count |
                /// -----------------------------------------------------
                auto current_name = stat.first;
                auto current_stat = stat.second;
                size_t entry_size = current_name.size() + sizeof(SliceStats);
                writer->write(reinterpret_cast<const char *>(&entry_size), sizeof(entry_size));
                writer->write(reinterpret_cast<const char *>(current_name.c_str()), current_name.size());
                writer->write(reinterpret_cast<const char *>(current_stat.get()), sizeof(SliceStats));
            }
            writer->finalize();
            slice_stats.clear();
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

public:
    void addSliceManagementCost(uint64_t cost) { slice_management_cost += cost; };

    void addSliceBufferCost(uint64_t cost) { slice_buffer_cost += cost; };

    void addSliceNextImplCost(uint64_t cost) { slice_next_impl_cost += cost; }

    void addSliceInitCost(uint64_t cost) { slice_init_cost += cost; }

private:
    /// SliceManagement() = default;
    SliceManagement() { total_space_size = 512UL * 1024 * 1024; };

    ~SliceManagement()
    {
        time_t now = time(nullptr);
        auto cstr_time = ctime(&now);
        String log_name = String(cstr_time, strlen(cstr_time) - 1) + ".slice_trace";
        for (size_t i = 0; i < log_name.size(); i++)
        {
            if (log_name[i] == ':' || log_name[i] == ' ')
            {
                log_name[i] = '_';
            }
        }
        stats.generateTraceToDisk(log_name, local_disk);
    }

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

    /// record cache stat
    CacheStats stats;

    std::atomic<uint64_t> slice_management_cost = 0;

    std::atomic<uint64_t> slice_next_impl_cost = 0;

    std::atomic<uint64_t> slice_buffer_cost = 0;

    std::atomic<uint64_t> slice_init_cost = 0;
};
};