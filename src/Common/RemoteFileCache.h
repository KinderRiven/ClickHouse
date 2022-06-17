#pragma once

#include <Common/FileCache.h>
#include <Common/logger_useful.h>

namespace DB
{

class RemoteFileCache : public IFileCache
{
public:
    RemoteFileCache(const String & cache_base_path_, const FileCacheSettings & cache_settings_);

    void initialize() override;

    void remove(const Key & key) override;

    void remove() override;

    std::vector<String> tryGetCachePaths(const Key & key) override;

    FileSegmentsHolder getOrSet(const Key & key, size_t offset, size_t size) override;

    FileSegmentsHolder get(const Key & key, size_t offset, size_t size) override;

    FileSegmentsHolder setDownloading(const Key & key, size_t offset, size_t size) override;

    FileSegments getSnapshot() const override;

    /// For debug.
    String dumpStructure(const Key & key) override;

    size_t getUsedCacheSize() const override;

    size_t getFileSegmentsNum() const override;

private:
    bool tryReserve(const Key & key, size_t offset, size_t size, std::lock_guard<std::mutex> & cache_lock) override;

    void remove(Key key, size_t offset, std::lock_guard<std::mutex> & cache_lock, std::lock_guard<std::mutex> & segment_lock) override;

    bool isLastFileSegmentHolder(
        const Key & key, size_t offset, std::lock_guard<std::mutex> & cache_lock, std::lock_guard<std::mutex> & segment_lock) override;

    void reduceSizeToDownloaded(
        const Key & key, size_t offset, std::lock_guard<std::mutex> & cache_lock, std::lock_guard<std::mutex> & segment_lock) override;

    FileSegments getImpl(const Key & key, size_t offset, size_t size);

    FileSegments
    splitRangeIntoCells(const Key & key, size_t offset, size_t size, FileSegment::State state, std::lock_guard<std::mutex> & cache_lock);

    void fillHolesWithEmptyFileSegments(
        FileSegments & file_segments,
        const Key & key,
        const FileSegment::Range & range,
        bool fill_with_detached_file_segments,
        std::lock_guard<std::mutex> & cache_lock);

private:
    Poco::Logger * log;
};

};
