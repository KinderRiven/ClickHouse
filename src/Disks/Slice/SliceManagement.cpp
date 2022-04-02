#include "SliceManagement.h"

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
        background_downloads_assignee = std::make_shared<CacheJobsAssignee>(context->getGlobalContext());
        background_downloads_assignee->start();
        hasInit = true;
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


std::shared_ptr<SliceManagement::SliceDownloadMetadata> SliceManagement::acquireDownloadSlice(const std::string & path)
{
    mutex.lock();
    background_downloads_assignee->scheduleDownloadTask();

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