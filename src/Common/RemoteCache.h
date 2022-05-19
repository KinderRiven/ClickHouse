#pragma once

#include <atomic>
#include <chrono>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <Core/Types.h>
#include <Disks/IDisk.h>
#include <boost/noncopyable.hpp>
#include <Common/FileSegment.h>
#include <Common/logger_useful.h>
#include "FileCache_fwd.h"

namespace DB
{

class IFileCache;
class RemoteCache;
using RemoteCachePtr = std::shared_ptr<RemoteCache>;

class RemoteCache : private boost::noncopyable
{
public:
    using Key = UInt128;

    RemoteCache(
        DiskPtr disk_, size_t upload_to_remote_cache_threshold_ = 5, size_t max_hits_element_ = 1024, size_t max_stash_element_ = 1024);

    void tryUploadToRemoteCache(IFileCache & cache, FileSegmentPtr file_segment);

    size_t getFileSegmentSizeFromRemoteCache(const Key & key, size_t offset);

    std::unique_ptr<ReadBufferFromFileBase> getReadBufferFromRemoteCache(const Key & key, size_t offset);

    size_t incrementFileSegmentHits(const Key & key, size_t offset);

    size_t getFileSegmentHits(const Key & key, size_t offset);

private:
    class LRUQueue
    {
    public:
        struct LRUQueueElement
        {
            Key key;
            size_t offset;
            size_t access;
        };

    public:
        using Iterator = typename std::list<LRUQueueElement>::iterator;

        size_t getElementsNum(std::lock_guard<std::mutex> & /* cache_lock */) const { return queue.size(); }

        Iterator add(FileSegmentPtr file_segment, std::lock_guard<std::mutex> & cache_lock);

        Iterator add(const Key & key, size_t offset, std::lock_guard<std::mutex> & cache_lock);

        void remove(Iterator queue_it, std::lock_guard<std::mutex> & cache_lock);

        void moveToEnd(Iterator queue_it, std::lock_guard<std::mutex> & cache_lock);

        bool contains(FileSegmentPtr file_segment, std::lock_guard<std::mutex> & cache_lock) const;

        Iterator begin() { return queue.begin(); }

        Iterator end() { return queue.end(); }

    private:
        std::list<LRUQueueElement> queue;
    };

    using KeyAndOffset = std::pair<Key, size_t>;
    using CacheMap = std::map<KeyAndOffset, LRUQueue::Iterator>;

private:
    String getPathInRemoteCache(const Key & key);

    String getPathInRemoteCache(const Key & key, size_t offset);

    void downloadToRemote(IFileCache & cache, FileSegmentPtr file_segment, std::lock_guard<std::mutex> & lock);

    CacheMap stash_map;
    LRUQueue stash_queue;

    CacheMap hits_map;
    LRUQueue hits_queue;

    DiskPtr disk;
    size_t max_hits_element;
    size_t max_stash_element;

    size_t upload_threshold;

    mutable std::mutex mutex;
    Poco::Logger * log;
};

};
