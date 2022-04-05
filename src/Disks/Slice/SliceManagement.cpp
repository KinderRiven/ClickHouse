#include "SliceManagement.h"

#define USE_LRU

namespace DB
{

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
        background_downloads_assignee = std::make_shared<CacheJobsAssignee>(CacheTaskType::CACHE_DOWNLOAD, context->getGlobalContext());
        background_downloads_assignee->start();
        background_cleanup_assignee = std::make_shared<CacheJobsAssignee>(CacheTaskType::CACHE_CLEANUP, context->getGlobalContext());
        background_cleanup_assignee->start();
        hasInit = true;
    }
}


void SliceManagement::traverseToLoad(const String & path)
{
    mutex.lock();
    size_t total_slice_size = 0;
    LOG_TRACE(log, "loading exist caching slice file from path {}.", path);
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
                auto metadata = std::make_shared<SliceDownloadMetadata>(file_size);
                main_list.push_back(std::make_pair(slice_fp, metadata)); /// push to main list
                metadata->setDownloaded();

                auto end_iter = main_list.end();
                std::advance(end_iter, -1);
                slice_downloads[slice_fp] = end_iter; /// create index

                total_slice_size += file_size;
                LOG_TRACE(log, "loading exist caching slice file {} / {}", fp, slice_fp);
            }
        }
    }
    usage_space_size = total_slice_size;
    total_slice_size /= (1024 * 1024);
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
    LOG_TRACE(log, "try to read {} from remote.", key);
    if (remote_disk != nullptr)
    {
        if (remote_disk->exists(key))
        {
            LOG_TRACE(log, "{} exist.", key);
            return std::move(remote_disk->readFile(key, settings, size));
        }
        else
        {
            LOG_TRACE(log, "{} don't exist.", key);
            return nullptr;
        }
    }
    else
    {
        LOG_TRACE(log, "empty remote disk.");
        return nullptr;
    }
}


std::shared_ptr<SliceDownloadMetadata> SliceManagement::acquireDownloadSlice(const std::string & path)
{
    mutex.lock();
    auto it = slice_downloads.find(path);
    if (it != slice_downloads.end())
    {
        auto metadata = it->second->second;
#ifdef USE_LRU
        main_list.erase(it->second);
        main_list.push_back(std::make_pair(path, metadata));
        auto end_iter = main_list.end();
        std::advance(end_iter, -1);
        it->second = end_iter;
#endif
        metadata->Access();
        mutex.unlock();
        return metadata;
    }
    /// create new metadata entry
    size_t slice_size = 4UL * 1024 * 1024;
    auto metadata = std::make_shared<SliceDownloadMetadata>(slice_size);
    main_list.push_back(std::make_pair(path, metadata)); /// add to main_list
    auto end_iter = main_list.end();
    std::advance(end_iter, -1);
    slice_downloads[path] = end_iter; /// create index
    metadata->Access();

    /// TODO start background cleanup task.
    usage_space_size += slice_size;
    if (usage_space_size > total_space_size)
    {
        tryToAddBackgroundCleanupTask();
    }
    mutex.unlock();
    return metadata;
}


std::shared_ptr<SliceDownloadMetadata> SliceManagement::tryToAddBackgroundDownloadTask(const String & path, int slice_id)
{
    auto metadata = acquireDownloadSlice(path);
    bool try_lock = metadata->tryLock();
    if (try_lock)
    {
        if (metadata->canDownload())
        {
            metadata->setPrefetch();
            LOG_TRACE(log, "Start a prefetch task, path:{}, slice_id:{}.", path, slice_id);
            background_downloads_assignee->addDownloadTask(path, slice_id, metadata);
            metadata->Unlock();
            return metadata;
        }
        else
        {
            metadata->Unlock();
            return nullptr;
        }
    }
    return nullptr;
}


void SliceManagement::cleanupMainList()
{
    mutex.lock();
    size_t usage_space_mb = usage_space_size / (1024 * 1024);
    size_t total_space_mb = total_space_size / (1024 * 1024);
    LOG_TRACE(
        log,
        "Using FIFO to cleanup slice cache start, file : {}/{}, usage : {}/{}MB.",
        main_list.size(),
        slice_downloads.size(),
        usage_space_mb,
        total_space_mb);
    {
        size_t max_free_space_size = (128UL * 1024 * 1024);
        size_t sz = main_list.size();
        size_t free_space_size = 0;
        for (size_t i = 0; i < sz; i++)
        {
            main_list.front().second->Lock();
            {
                slice_downloads.erase(main_list.front().first);
                local_disk->removeFile(main_list.front().first);
                free_space_size += main_list.front().second->size;
                main_list.front().second->setDelete();
                LOG_TRACE(log, "Using FIFO to rm slice {}, access {}.", main_list.front().first, main_list.front().second->access);
            }
            main_list.front().second->Unlock();
            main_list.pop_front();
            if (free_space_size >= max_free_space_size)
            {
                break;
            }
        }
        usage_space_size -= free_space_size;
        is_cleanup = false;
    }
    usage_space_mb = usage_space_size / (1024 * 1024);
    total_space_mb = total_space_size / (1024 * 1024);
    LOG_TRACE(
        log,
        "Using FIFO to cleanup slice cache finished, file : {}/{}, usage : {}/{}MB.",
        main_list.size(),
        slice_downloads.size(),
        usage_space_mb,
        total_space_mb);
    mutex.unlock();
}


void SliceManagement::tryToAddBackgroundCleanupTask()
{
    if (!is_cleanup)
    {
        is_cleanup = true;
        background_cleanup_assignee->addCleanupTask();
    }
}

};