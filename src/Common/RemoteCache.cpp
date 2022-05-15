#include "RemoteCache.h"
#include <IO/ReadBufferFromFile.h>
#include <IO/copyData.h>
#include <Common/FileCache.h>
#include <Common/hex.h>

namespace DB
{

namespace
{
    String keyToStr(const RemoteCache::Key & key) { return getHexUIntLowercase(key); }
}

RemoteCache::RemoteCache(DiskPtr disk_)
    : disk(disk_), max_stash_element(1024), download_threshold(3), log(&Poco::Logger::get("RemoteCache"))
{
    if (disk != nullptr)
        LOG_INFO(log, "remote cache disk {}", disk->getName());
}

void RemoteCache::add(IFileCache & cache, FileSegmentPtr segment)
{
    std::lock_guard<std::mutex> lock(mutex);
    auto queue_it = stash.find({segment->key(), segment->offset()});

    if (queue_it != stash.end())
    {
        queue_it->second->access++;
        if (queue_it->second->access >= download_threshold)
        {
            LOG_INFO(
                log,
                "downloading file_segment {}-{}-{} to remote cache.",
                keyToStr(segment->key()),
                segment->offset(),
                queue_it->second->access);
            downloadToRemote(cache, segment);
        }
        queue.moveToEnd(queue_it->second, lock);
    }
    else
    {
        LOG_INFO(
            log,
            "add file_segment {}-{} to stash [{}/{}].",
            keyToStr(segment->key()),
            segment->offset(),
            queue.getElementsNum(lock),
            max_stash_element);

        auto iter = queue.add(segment, lock);
        stash.insert({{segment->key(), segment->offset()}, iter});
        if (queue.getElementsNum(lock) >= max_stash_element)
        {
            auto rm_queue_iter = queue.begin();
            auto rm_segment = rm_queue_iter->segment;
            auto rm_map_iter = stash.find({rm_segment->key(), rm_segment->offset()});
            stash.erase(rm_map_iter);
            queue.remove(rm_queue_iter, lock);
        }
    }
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

void RemoteCache::downloadToRemote(IFileCache & cache, FileSegmentPtr segment)
{
    auto local_path = cache.getPathInLocalCache(segment->key(), segment->offset());
    auto local_cache_reader = std::make_unique<ReadBufferFromFile>(local_path);

    auto remote_path = getPathInRemoteCache(segment->key(), segment->offset());
    if (!disk->exists(remote_path))
    {
        disk->createDirectories(getPathInRemoteCache(segment->key()));
        auto remote_cache_writer = disk->writeFile(remote_path, 1048576UL, WriteMode::Rewrite, {});
        LOG_INFO(log, "local_path :{}, remote_path : {}", local_path, remote_path);
        copyData(*local_cache_reader, *remote_cache_writer);
    }
}

RemoteCache::LRUQueue::Iterator RemoteCache::LRUQueue::add(FileSegmentPtr file_segment, std::lock_guard<std::mutex> &)
{
    LRUQueue::LRUQueueElement elem{
        .segment = file_segment,
        .access = 0,
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
