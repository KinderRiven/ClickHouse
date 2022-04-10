#include "SliceManagement.h"
#include <IO/copyData.h>

namespace DB
{

/// main list name
/// list_map["main"] = main_list;
static String main_list_name = "main";

/// prefetch list name
/// list_map["prefetch"] = prefetch_list;
static String prefetch_list_name = "prefetch";

/// max prefetch list size
static size_t max_prefetch_size = (512UL * 1024 * 1024);
/// static size_t max_prefetch_size = (64UL * 1024 * 1024);

/// max list size for each query
static size_t max_query_cache_size = (2UL * 1024 * 1024 * 1024);
/// static size_t max_query_cache_size = (128UL * 1024 * 1024);

/// max main list size
static size_t max_cache_size = (10UL * 1024 * 1024 * 1024);
/// static size_t max_cache_size = (512UL * 1024 * 1024);


String GetLocalSlicePath(const String & path, int slice_id)
{
    String slice_path = path + ".slice" + "_" + std::to_string(slice_id);
    return slice_path;
}


SliceManagement & SliceManagement::instance()
{
    static SliceManagement ret;
    return ret;
}


void SliceManagement::initlizate(ContextPtr context_)
{
    if (!hasInit)
    {
        context = context_;
        background_prefetch_assignee = std::make_shared<CacheJobsAssignee>(CacheTaskType::CACHE_PREFETCH, context->getGlobalContext());
        background_prefetch_assignee->start();
        background_cleanup_assignee = std::make_shared<CacheJobsAssignee>(CacheTaskType::CACHE_CLEANUP, context->getGlobalContext());
        background_cleanup_assignee->start();
        hasInit = true;
    }
}


void SliceManagement::traverseToLoad(const String & path)
{
    mutex.lock();
    LOG_TRACE(log, "loading exist caching slice file from path {}.", path);
    /// creat main list
    auto main_list = std::make_shared<SliceManagement::SliceListMetadata>(main_list_name, max_cache_size);
    list_map[main_list_name] = main_list;
    /// create prefetch list
    auto prefetch_list = std::make_shared<SliceManagement::SliceListMetadata>(prefetch_list_name, max_prefetch_size);
    list_map[prefetch_list_name] = prefetch_list;

    for (const fs::directory_entry & dir_entry : fs::recursive_directory_iterator(path))
    {
        if (dir_entry.is_regular_file())
        {
            String fp = dir_entry.path().relative_path();
            auto pos = fp.find("store/");
            if (pos != std::string::npos)
            {
                String slice_fp = fp.substr(pos);
                size_t file_size = dir_entry.file_size();
                auto metadata = std::make_shared<SliceDownloadMetadata>(main_list_name, file_size);
                metadata->setDownloaded();
                main_list->PushBack(slice_fp, metadata); /// push to main_list
                slice_downloads[slice_fp] = main_list->LastIterator(); /// add to global_index
                LOG_TRACE(log, "loading exist caching slice file {} / {}", fp, slice_fp);
            }
        }
    }
    size_t total_slice_size = (main_list->UsageBytes() / (1024 * 1024));
    LOG_TRACE(
        log, "loading exist caching slice file finished, total count : {}, total size : {} MB.", slice_downloads.size(), total_slice_size);
    mutex.unlock();
}


void SliceManagement::setupLocalCacheDisk(std::shared_ptr<IDisk> local_disk_)
{
    if (local_disk_ != nullptr)
    {
        LOG_TRACE(log, "setup local cache disk : {}", local_disk_->getName());
        local_disk = std::move(local_disk_);
        /// TODO load local caching slice file
        if (!local_disk->exists("store/"))
        {
            local_disk->createDirectories("store/");
        }
        traverseToLoad(local_disk->getPath());
    }
    else
    {
        LOG_TRACE(log, "setup local cache disk failed, because remote_disk ptr is nullptr!");
    }
}


void SliceManagement::setupRemoteCacheDisk(std::shared_ptr<IDisk> remote_disk_)
{
    if (remote_disk_ != nullptr)
    {
        LOG_TRACE(log, "setup remote cache disk : {}", remote_disk_->getName());
    }
    else
    {
        LOG_TRACE(log, "setup remote cache disk failed, because remote_disk ptr is nullptr!");
    }
    remote_disk = std::move(remote_disk_);
}


std::unique_ptr<WriteBufferFromFileBase> SliceManagement::createRemoteFileToUpload(const String & path, size_t buf_size, WriteMode mode)
{
    if (remote_disk != nullptr)
    {
        return std::move(remote_disk->writeFile(path, buf_size, mode));
    }
    else
    {
        return nullptr;
    }
}


std::unique_ptr<ReadBufferFromFileBase>
SliceManagement::tryToReadSliceFromRemote(const String & key, const ReadSettings & settings, std::optional<size_t> size)
{
#ifdef SLICE_DEBUG
    LOG_TRACE(log, "try to read {} from remote.", key);
#endif
    if (remote_disk != nullptr)
    {
        if (remote_disk->exists(key))
        {
#ifdef SLICE_DEBUG
            LOG_TRACE(log, "{} exist.", key);
#endif
            return std::move(remote_disk->readFile(key, settings, size));
        }
        else
        {
#ifdef SLICE_DEBUG
            LOG_TRACE(log, "{} don't exist.", key);
#endif
            return nullptr;
        }
    }
    else
    {
#ifdef SLICE_DEBUG
        LOG_TRACE(log, "empty remote disk.");
#endif
        return nullptr;
    }
}


SliceManagement::SlicePtr SliceManagement::acquireDownloadSlice(String & query_id, const std::string & path)
{
    mutex.lock();
    auto it = slice_downloads.find(path);
    /// This slice has been created.
    if (it != slice_downloads.end())
    {
        stats.addSliceHit(path);
        stats.addQueryHit(query_id);

        String list_name = it->second->second->query_id;
        auto metadata = it->second->second;
        /// This slice in prefetch list, thus, we need to move it to query_id_list.
        if (list_name == prefetch_list_name)
        {
            if (query_id != prefetch_list_name)
            {
                stats.addPrefetchHit();
            }
            auto from_list = list_map[prefetch_list_name];
            auto to_list = list_map[query_id];
#ifdef SLICE_DEBUG
            /// DEBUG
            LOG_TRACE(
                log,
                "hit prefetch cache {}, then move it to {}, current prefetch cache {}/{}MB.",
                path,
                query_id,
                from_list->UsageMB(),
                from_list->TotalMB());
#endif
            /// TODO move
            from_list->Erase(it->second);
            to_list->PushBack(path, metadata);
            metadata->setQueryId(query_id); /// reset its list to current query list.
            auto end_iter = to_list->LastIterator();
            it->second = end_iter; /// update global_index
        }
        /// This slice in query_id_list or main_list.
        else
        {
            auto query_list = list_map[list_name];
            /// LRU
            query_list->Erase(it->second);
            query_list->PushBack(path, metadata);
            auto end_iter = query_list->LastIterator();
            it->second = end_iter; /// update global_index
        }
        /// update access count for LFU
        metadata->Access();
        mutex.unlock();
        return metadata;
    }
    else
    {
        stats.addSliceMiss(path);
        stats.addQueryMiss(query_id);

        /// create new metadata entry
        size_t slice_size = 4UL * 1024 * 1024;
        auto metadata = std::make_shared<SliceDownloadMetadata>(query_id, slice_size);
        auto query_list = list_map[metadata->query_id];
        /// LRU
        query_list->PushBack(path, metadata); /// add to main_list
        auto end_iter = query_list->LastIterator();
        slice_downloads[path] = end_iter; /// create index
        /// update access count for LFU
        metadata->Access();
        /// maybe cleanup task.
        if (query_list->isFull())
        {
            tryToFreeListSpace(query_list);
        }
        mutex.unlock();
        return metadata;
    }
}


void SliceManagement::handlePrefetch()
{
    prefetch_mutex.lock();
    while (!prefetch_queue.empty())
    {
        auto front = prefetch_queue.front();
        auto reader = front->remote_disk->readFile(front->filename, front->read_settings, 0);
        for (size_t i = 0; i < front->vec_slice.size(); i++)
        {
            String path = GetLocalSlicePath(front->filename, std::get<0>(front->vec_slice[i]));
            auto metadata = acquireDownloadSlice(prefetch_list_name, path);
            bool try_to_lock = metadata->tryLock();
            if (try_to_lock)
            {
                if (metadata->canDownload())
                {
                    stats.addPrefetch(1);
#ifdef SLICE_DEBUG
                    /// debug print
                    LOG_TRACE(
                        log,
                        "we can prefetch slice : [{}]/[{}]-[{}]-[{}].",
                        front->filename,
                        std::get<0>(front->vec_slice[i]),
                        std::get<1>(front->vec_slice[i]),
                        std::get<2>(front->vec_slice[i]));
#endif
                    size_t offset = std::get<1>(front->vec_slice[i]);
                    size_t size = std::get<2>(front->vec_slice[i]);

                    /// create directory if not exists.
                    metadata->setPrefetch();
                    auto dir_path = directoryPath(path);
                    if (!local_disk->exists(dir_path))
                    {
                        local_disk->createDirectories(dir_path);
                    }
                    /// copy
                    String tmp_path = path + ".tmp";
                    auto writer = local_disk->writeFile(tmp_path, front->read_settings.local_fs_buffer_size, WriteMode::Rewrite);
                    reader->seek(static_cast<off_t>(offset), SEEK_SET);
                    copyData(*reader, *writer, size);

                    /// atmoic move
                    local_disk->moveFile(tmp_path, path);
                    metadata->setDownloaded();
                }
                else
                {
#ifdef SLICE_DEBUG
                    LOG_TRACE(
                        log,
                        "we can not prefetch slice : [{}]/[{}]-[{}]-[{}], because it has been downloaded",
                        front->filename,
                        std::get<0>(front->vec_slice[i]),
                        std::get<1>(front->vec_slice[i]),
                        std::get<2>(front->vec_slice[i]));
#endif
                }
                /// else may be any other thread is downloading or downloaded.
                metadata->Unlock();
            }
            else
            {
#ifdef SLICE_DEBUG
                LOG_TRACE(
                    log,
                    "we can not prefetch slice : [{}]/[{}]-[{}]-[{}], because it has been lock",
                    front->filename,
                    std::get<0>(front->vec_slice[i]),
                    std::get<1>(front->vec_slice[i]),
                    std::get<2>(front->vec_slice[i]));
#endif
            }
        }
        prefetch_queue.pop();
    }
    prefetch_mutex.unlock();
    is_prefetch = false;
}


void SliceManagement::tryToAddBackgroundPrefetchTask(std::shared_ptr<SlicePrefetchTask> task)
{
    if (!is_prefetch)
    {
        is_prefetch = true;
        prefetch_mutex.lock();
        prefetch_queue.push(task);
        prefetch_mutex.unlock();
        background_prefetch_assignee->trigger();
    }
}


void SliceManagement::tryToFreeListSpace(std::shared_ptr<SliceManagement::SliceListMetadata> list)
{
#ifdef SLICE_DEBUG
    LOG_TRACE(log, "[start] Using LRU to cleanup {} list, usage : {}/{}MB.", list->ListName(), list->UsageMB(), list->TotalMB());
#endif
    {
        size_t need_to_free = list->UsageBytes() - list->TotalBytes();
        size_t has_free = 0;
        size_t sz = list->SliceCount();
        for (size_t i = 0; i < sz; i++)
        {
            auto begin_iter = list->FirstIterator();
            auto path = begin_iter->first;
            auto metadata = begin_iter->second;
            metadata->Lock();
            if (!metadata->NumRef())
            {
                metadata->setDelete();
                slice_downloads.erase(path);
                local_disk->removeFile(path);
                has_free += metadata->size;
                list->PopFront();
            }
            else
            {
                list->PushBack(path, metadata);
                slice_downloads[path] = list->LastIterator();
                list->PopFront();
            }
            metadata->Unlock();
            if (has_free >= need_to_free)
            {
                break;
            }
        }
    }
#ifdef SLICE_DEBUG
    LOG_TRACE(log, "[end] Using LRU to cleanup {} list, usage : {}/{}MB.", list->ListName(), list->UsageMB(), list->TotalMB());
#endif
}


void SliceManagement::cleanupMainList()
{
    mutex.lock();
    auto main_list = list_map[main_list_name];
    auto prefetch_list = list_map[prefetch_list_name];

#ifdef SLICE_DEBUG
    LOG_TRACE(
        log,
        "[start] Using LRU to cleanup prefetch list : {}, usage : {}/{}MB.",
        prefetch_list->SliceCount(),
        prefetch_list->UsageMB(),
        prefetch_list->TotalMB());
    LOG_TRACE(
        log,
        "[start] Using LRU to cleanup main list, file : {}/{}, usage : {}/{}MB.",
        main_list->SliceCount(),
        slice_downloads.size(),
        main_list->UsageMB(),
        main_list->TotalMB());
#endif
    {
        size_t max_free_space_size = (128UL * 1024 * 1024);
        size_t sz = main_list->SliceCount();
        size_t free_space_size = 0;
        for (size_t i = 0; i < sz; i++)
        {
            auto begin_iter = main_list->FirstIterator();
            auto path = begin_iter->first;
            auto metadata = begin_iter->second;
            metadata->Lock();
            if (!metadata->NumRef())
            {
                metadata->setDelete();
                slice_downloads.erase(path);
                local_disk->removeFile(path);
                free_space_size += metadata->size;
                main_list->PopFront();
            }
            else
            {
                main_list->PushBack(path, metadata);
                slice_downloads[path] = main_list->LastIterator();
                main_list->PopFront();
            }
            metadata->Unlock();
            if (free_space_size >= max_free_space_size)
            {
                break;
            }
        }
        is_cleanup = false;
    }
#ifdef SLICE_DEBUG
    LOG_TRACE(
        log,
        "[end] Using LRU to cleanup prefetch list : {}, usage : {}/{}MB.",
        prefetch_list->SliceCount(),
        prefetch_list->UsageMB(),
        prefetch_list->TotalMB());
    LOG_TRACE(
        log,
        "[end] Using LRU to cleanup main list, file : {}/{}, usage : {}/{}MB.",
        main_list->SliceCount(),
        slice_downloads.size(),
        main_list->UsageMB(),
        main_list->TotalMB());
#endif
    mutex.unlock();
}


void SliceManagement::tryToAddBackgroundCleanupTask()
{
    if (!is_cleanup)
    {
        is_cleanup = true;
        background_cleanup_assignee->trigger();
    }
}


void SliceManagement::setQueryContext(String & query_id)
{
    mutex.lock();
    auto it = list_map.find(query_id);
    if (it != list_map.end())
    {
        it->second->SubRef();
#ifdef SLICE_DEBUG
        LOG_TRACE(log, "[set query context][update][query_id:{}][num_open_file:{}]", query_id, it->second->NumRef());
#endif
    }
    else
    {
        /// this is new query
        auto new_list = std::make_shared<SliceManagement::SliceListMetadata>(query_id, max_query_cache_size);
        list_map[query_id] = new_list;
        new_list->SubRef();
#ifdef SLICE_DEBUG
        LOG_TRACE(log, "[set query context][open:{}][query_id:{}][num_open_file:{}]", query_id, list_map.size(), new_list->NumRef());
#endif
    }
    mutex.unlock();
}


void SliceManagement::listMove(
    std::shared_ptr<SliceManagement::SliceListMetadata> from, std::shared_ptr<SliceManagement::SliceListMetadata> to)
{
#ifdef SLICE_DEBUG
    LOG_TRACE(
        log,
        "start move list from [query_id:{}][size:{}] to [query_id:{}][size:{}].",
        from->ListName(),
        from->SliceCount(),
        to->ListName(),
        to->SliceCount());
#endif
    auto from_list = from->List();
    for (auto iter = from_list->begin(); iter != from_list->end(); iter++)
    {
        /// modify slice query_id.
        to->PushBack(iter->first, iter->second);
        iter->second->setQueryId(to->ListName());
        /// update global index
        auto end_iter = to->LastIterator();
        slice_downloads[iter->first] = end_iter; /// create index
    }
#ifdef SLICE_DEBUG
    LOG_TRACE(
        log,
        "finish move list from [query_id:{}][size:{}] to [query_id:{}][size:{}].",
        from->ListName(),
        from->SliceCount(),
        to->ListName(),
        to->SliceCount());
#endif
}


void SliceManagement::freeQueryContext(String & query_id)
{
    mutex.lock();
    auto it = list_map.find(query_id);
    if (it != list_map.end())
    {
        auto query_list = it->second;
        query_list->DecRef();
#ifdef SLICE_DEBUG
        LOG_TRACE(log, "[free query context][delete][query_id:{}][num_open_file:{}]", query_id, query_list->NumRef());
#endif
        if (!query_list->NumRef())
        {
            auto from_list = list_map[query_id];
            auto to_list = list_map[main_list_name];
            listMove(from_list, to_list);
#ifdef SLICE_DEBUG
            /// debug to print query statistics.
            LOG_TRACE(
                log,
                "[query statistic][query:{}][cache_hit:{}][prefetch hit:{}]",
                query_id,
                stats.getQueryHitRatio(query_id),
                stats.getPrefetchHitRatio());
#endif
            list_map.erase(query_id);
            /// try to start a background GC task.
            if (to_list->isFull())
            {
                tryToAddBackgroundCleanupTask();
            }
        }
    }
    mutex.unlock();
}
};