#include "RemoteCache.h"
#include <IO/ReadBufferFromFile.h>
#include <IO/copyData.h>
#include <Common/FileCache.h>
#include <Common/hex.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    String keyToStr(const RemoteCache::Key & key) { return getHexUIntLowercase(key); }
}

RemoteCache::RemoteCache(DiskPtr disk_, size_t upload_to_remote_cache_threshold_, size_t max_hits_element_, size_t max_stash_element_)
    : disk(disk_)
    , max_hits_element(max_hits_element_)
    , max_stash_element(max_stash_element_)
    , upload_threshold(upload_to_remote_cache_threshold_)
    , log(&Poco::Logger::get("RemoteCache"))
{
    if (disk != nullptr)
        LOG_INFO(log, "remote cache disk {}", disk->getName());
}

void RemoteCache::tryUploadToRemoteCache(IFileCache & cache, FileSegmentPtr file_segment)
{
    if (!disk || !file_segment->supportRemoteCache() || file_segment->stateUnlock() != FileSegment::State::DOWNLOADED)
        return;

    std::lock_guard<std::mutex> lock(mutex);
    auto queue_it = stash_map.find({file_segment->key(), file_segment->offset()});

    if (queue_it != stash_map.end())
    {
        queue_it->second->access++;
        if (queue_it->second->access >= upload_threshold)
        {
            downloadToRemote(cache, file_segment, lock);
        }
        stash_queue.moveToEnd(queue_it->second, lock);
    }
    else
    {
        auto iter = stash_queue.add(file_segment, lock);
        stash_map.insert({{file_segment->key(), file_segment->offset()}, iter});

        if (stash_queue.getElementsNum(lock) >= max_stash_element)
        {
            auto rm_queue_iter = stash_queue.begin();
            auto rm_map_iter = stash_map.find({rm_queue_iter->key, rm_queue_iter->offset});
            stash_map.erase(rm_map_iter);
            stash_queue.remove(rm_queue_iter, lock);
        }

        if (iter->access >= upload_threshold)
            downloadToRemote(cache, file_segment, lock);
    }
}

size_t RemoteCache::getFileSegmentSizeFromRemoteCache(const Key & key, size_t offset)
{
    if (!disk)
        return 0;
    {
        std::lock_guard<std::mutex> lock(mutex);
        auto path = getPathInRemoteCache(key, offset);
        if (disk->exists(path))
            return disk->getFileSize(path);
        else
            return 0;
    }
}

std::unique_ptr<ReadBufferFromFileBase> RemoteCache::getReadBufferFromRemoteCache(const Key & key, size_t offset)
{
    if (!disk)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Unable to load cache from non-existent remote cache disk, [key:{}][offset:{}].",
            keyToStr(key),
            offset);
    {
        std::lock_guard<std::mutex> lock(mutex);
        auto path = getPathInRemoteCache(key, offset);
        if (disk->exists(path))
        {
            return disk->readFile(path);
        }
        else
            return nullptr;
    }
}

size_t RemoteCache::incrementFileSegmentHits(const Key & key, size_t offset)
{
    std::lock_guard<std::mutex> lock(mutex);

    auto queue_it = hits_map.find({key, offset});

    if (queue_it != hits_map.end())
    {
        queue_it->second->access++;
        return queue_it->second->access;
    }
    else
    {
        auto iter = hits_queue.add(key, offset, lock);
        hits_map.insert({{key, offset}, iter});
        if (hits_queue.getElementsNum(lock) >= max_hits_element)
        {
            auto rm_queue_iter = hits_queue.begin();
            auto rm_map_iter = hits_map.find({rm_queue_iter->key, rm_queue_iter->offset});
            hits_map.erase(rm_map_iter);
            hits_queue.remove(rm_queue_iter, lock);
        }
        return 0;
    }
}

size_t RemoteCache::getFileSegmentHits(const Key & key, size_t offset)
{
    std::lock_guard<std::mutex> lock(mutex);

    auto queue_it = hits_map.find({key, offset});
    return (queue_it != hits_map.end()) ? queue_it->second->access : 0;
}

String RemoteCache::getPathInRemoteCache(const Key & key, size_t offset)
{
    auto key_str = keyToStr(key);
    return fs::path(key_str.substr(0, 3)) / key_str / std::to_string(offset);
}

String RemoteCache::getPathInRemoteCache(const Key & key)
{
    auto key_str = keyToStr(key);
    return fs::path(key_str.substr(0, 3)) / key_str;
}

void RemoteCache::downloadToRemote(IFileCache & cache, FileSegmentPtr file_segment, std::lock_guard<std::mutex> &)
{
    auto local_path = cache.getPathInLocalCache(file_segment->key(), file_segment->offset());
    auto local_cache_reader = std::make_unique<ReadBufferFromFile>(local_path);
    auto remote_path = getPathInRemoteCache(file_segment->key(), file_segment->offset());

    if (!disk->exists(remote_path))
    {
        disk->createDirectories(getPathInRemoteCache(file_segment->key()));
        auto remote_cache_writer = disk->writeFile(remote_path, 1048576UL, WriteMode::Rewrite, {});
        LOG_INFO(log, "local_path :{}, remote_path : {}", local_path, remote_path);
        copyData(*local_cache_reader, *remote_cache_writer);
    }
}

RemoteCache::LRUQueue::Iterator RemoteCache::LRUQueue::add(FileSegmentPtr file_segment, std::lock_guard<std::mutex> &)
{
    LRUQueue::LRUQueueElement elem{
        .key = file_segment->key(),
        .offset = file_segment->offset(),
        .access = 0,
    };
    return queue.insert(queue.end(), elem);
}

RemoteCache::LRUQueue::Iterator RemoteCache::LRUQueue::add(const Key & key, size_t offset, std::lock_guard<std::mutex> &)
{
    LRUQueue::LRUQueueElement elem{
        .key = key,
        .offset = offset,
        .access = 1,
    };
    return queue.insert(queue.end(), elem);
}

void RemoteCache::LRUQueue::remove(Iterator queue_it, std::lock_guard<std::mutex> &)
{
    queue.erase(queue_it);
}

void RemoteCache::LRUQueue::moveToEnd(Iterator queue_it, std::lock_guard<std::mutex> &)
{
    queue.splice(queue.end(), queue, queue_it);
}

bool RemoteCache::LRUQueue::contains(FileSegmentPtr, std::lock_guard<std::mutex> &) const
{
    return false;
}

};
