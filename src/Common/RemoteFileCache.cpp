#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <Common/RemoteFileCache.h>
#include <Common/hex.h>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int REMOTE_FS_OBJECT_CACHE_ERROR;
    extern const int LOGICAL_ERROR;
}

namespace
{
    String keyToStr(const IFileCache::Key & key)
    {
        return getHexUIntLowercase(key);
    }
}

inline UInt64 time_in_microseconds(std::chrono::time_point<std::chrono::system_clock> timepoint)
{
    return std::chrono::duration_cast<std::chrono::microseconds>(timepoint.time_since_epoch()).count();
}

inline String get_time_string()
{
    time_t timep;
    time(&timep);
    char tmp[64];
    strftime(tmp, sizeof(tmp), "%Y_%m_%d_%H_%M_%S", localtime(&timep));
    return tmp;
}

RemoteFileCache::RemoteFileCache(const String & cache_base_path_, const FileCacheSettings & cache_settings_)
    : IFileCache(cache_base_path_, cache_settings_), log(&Poco::Logger::get("RemoteFileCache"))
{
}

void RemoteFileCache::initialize()
{
}

void RemoteFileCache::remove(const Key &)
{
    /// NOTHING TO DO
}

void RemoteFileCache::remove()
{
    /// NOTHING TO DO
}

std::vector<String> RemoteFileCache::tryGetCachePaths(const Key &)
{
    return {};
}

void RemoteFileCache::appendCacheLogEntry(const Key & key, size_t offset, size_t size, std::lock_guard<std::mutex> &)
{
    cache_log.emplace_back(RemoteFileCache::CacheLogEntry(key, offset, size));
}

String RemoteFileCache::getCacheLogPath()
{
    auto log_base_path = fs::path(cache_base_path) / "cache_log";

    if (!fs::exists(log_base_path))
        fs::create_directories(log_base_path);

    return fs::path(log_base_path) / get_time_string();
}

void RemoteFileCache::outputCacheLogIfNeeded(std::lock_guard<std::mutex> &)
{
    if (cache_log.size() > 1000UL)
    {
        std::ofstream ostr(getCacheLogPath());
        for (const auto & cache_log_entry : cache_log)
        {
            ostr << keyToStr(cache_log_entry.key) << "," << cache_log_entry.offset << "," << cache_log_entry.size << ","
                 << cache_log_entry.used_time << std::endl;
        }
        ostr.close();
        cache_log.clear();
    }
}

void RemoteFileCache::dumpCacheLog()
{
    for (const auto & cache_log_entry : cache_log)
    {
        LOG_INFO(
            log,
            "[key:{}][offset:{}][size:{}][time:{}]",
            keyToStr(cache_log_entry.key),
            cache_log_entry.offset,
            cache_log_entry.size,
            cache_log_entry.used_time);
    }
}

FileSegments RemoteFileCache::getImpl(const Key & key, size_t offset, size_t size, std::lock_guard<std::mutex> & cache_lock)
{
    FileSegments segments;
    /// sort with offset
    std::map<unsigned long, size_t> files;
    auto cache_path = getPathInLocalCache(key);

    if (fs::exists(cache_path))
    {
        for (const fs::directory_entry & entry : fs::directory_iterator(fs::path(cache_path)))
        {
            std::string file_name = entry.path().filename();
            unsigned long file_offset = std::stoul(file_name);
            size_t file_size = fs::file_size(entry.path());
            files.insert({file_offset, file_size});
        }
        for (auto [file_offset, file_size] : files)
        {
            auto left = file_offset;
            auto right = file_offset + file_size - 1;
            ///   |---------------------------------|
            /// |--1---|    |--2--|              |-----2-----|
            /// |-----------------1--------------------|
            if (((left <= offset) && (right >= offset)) || ((left >= offset) && (left < (offset + size))))
            {
                LOG_INFO(log, "getImpl [key:{}][range({},{})][OK]", keyToStr(key), left, right);
                auto file_segment = std::make_shared<FileSegment>(file_offset, file_size, key, this, FileSegment::State::DOWNLOADED);
                appendCacheLogEntry(key, file_offset, file_size, cache_lock);
                segments.emplace_back(file_segment);
            }
        }
    }
    return segments;
}

FileSegments RemoteFileCache::splitRangeIntoCells(
    const Key & key, size_t offset, size_t size, FileSegment::State, std::lock_guard<std::mutex> & cache_lock)
{
    assert(size > 0);

    /// auto key_path = getPathInLocalCache(key);
    /// if (!fs::exists(key_path))
    ///     fs::create_directories(key_path);

    auto current_pos = offset;
    auto end_pos_non_included = offset + size;

    size_t current_cell_size;
    size_t remaining_size = size;

    FileSegments file_segments;
    while (current_pos < end_pos_non_included)
    {
        current_cell_size = std::min(remaining_size, max_file_segment_size);
        remaining_size -= current_cell_size;

        auto file_segment = std::make_shared<FileSegment>(current_pos, current_cell_size, key, this, FileSegment::State::SKIP_CACHE);
        appendCacheLogEntry(key, current_pos, current_cell_size, cache_lock);
        file_segments.push_back(file_segment);
        current_pos += current_cell_size;
    }

    assert(file_segments.empty() || offset + size - 1 == file_segments.back()->range().right);
    return file_segments;
}

void RemoteFileCache::fillHolesWithEmptyFileSegments(
    FileSegments & file_segments,
    const Key & key,
    const FileSegment::Range & range,
    bool fill_with_detached_file_segments,
    std::lock_guard<std::mutex> & cache_lock)
{
    /// auto key_path = getPathInLocalCache(key);
    /// if (!fs::exists(key_path))
    ///     fs::create_directories(key_path);

    /// There are segments [segment1, ..., segmentN]
    /// (non-overlapping, non-empty, ascending-ordered) which (maybe partially)
    /// intersect with given range.

    /// It can have holes:
    /// [____________________]         -- requested range
    ///     [____]  [_]   [_________]  -- intersecting cache [segment1, ..., segmentN]
    ///
    /// For each such hole create a cell with file segment state EMPTY.

    auto it = file_segments.begin();
    auto segment_range = (*it)->range();

    size_t current_pos;
    if (segment_range.left < range.left)
    {
        ///    [_______     -- requested range
        /// [_______
        /// ^
        /// segment1

        current_pos = segment_range.right + 1;
        ++it;
    }
    else
        current_pos = range.left;

    while (current_pos <= range.right && it != file_segments.end())
    {
        segment_range = (*it)->range();

        if (current_pos == segment_range.left)
        {
            current_pos = segment_range.right + 1;
            ++it;
            continue;
        }

        assert(current_pos < segment_range.left);

        auto hole_size = segment_range.left - current_pos;

        if (fill_with_detached_file_segments)
        {
            auto file_segment = std::make_shared<FileSegment>(current_pos, hole_size, key, this, FileSegment::State::SKIP_CACHE);
            appendCacheLogEntry(key, current_pos, hole_size, cache_lock);
            {
                std::lock_guard segment_lock(file_segment->mutex);
                file_segment->markAsDetached(segment_lock);
            }
            file_segments.insert(it, file_segment);
        }
        else
        {
            file_segments.splice(it, splitRangeIntoCells(key, current_pos, hole_size, FileSegment::State::SKIP_CACHE, cache_lock));
        }

        current_pos = segment_range.right + 1;
        ++it;
    }

    if (current_pos <= range.right)
    {
        ///   ________]     -- requested range
        ///   _____]
        ///        ^
        /// segmentN

        auto hole_size = range.right - current_pos + 1;

        if (fill_with_detached_file_segments)
        {
            auto file_segment = std::make_shared<FileSegment>(current_pos, hole_size, key, this, FileSegment::State::SKIP_CACHE);
            appendCacheLogEntry(key, current_pos, hole_size, cache_lock);
            {
                std::lock_guard segment_lock(file_segment->mutex);
                file_segment->markAsDetached(segment_lock);
            }
            file_segments.insert(file_segments.end(), file_segment);
        }
        else
        {
            file_segments.splice(
                file_segments.end(), splitRangeIntoCells(key, current_pos, hole_size, FileSegment::State::SKIP_CACHE, cache_lock));
        }
    }
}

FileSegmentsHolder RemoteFileCache::getOrSet(const Key & key, size_t offset, size_t size)
{
    FileSegment::Range range(offset, offset + size - 1);

    std::lock_guard cache_lock(mutex);

    FileSegments file_segments = getImpl(key, offset, size, cache_lock);

    if (file_segments.empty())
    {
        file_segments = splitRangeIntoCells(key, offset, size, FileSegment::State::SKIP_CACHE, cache_lock);
    }
    else
    {
        fillHolesWithEmptyFileSegments(file_segments, key, range, false, cache_lock);
    }
    {
        outputCacheLogIfNeeded(cache_lock);
    }
    return FileSegmentsHolder(std::move(file_segments));
}

FileSegmentsHolder RemoteFileCache::get(const Key & key, size_t offset, size_t size)
{
    return getOrSet(key, offset, size);
}

FileSegmentsHolder RemoteFileCache::setDownloading(const Key &, size_t, size_t)
{
    FileSegments segments = {};
    return FileSegmentsHolder(std::move(segments));
}

FileSegments RemoteFileCache::getSnapshot() const
{
    return {};
}

String RemoteFileCache::dumpStructure(const Key &)
{
    return {};
}

size_t RemoteFileCache::getUsedCacheSize() const
{
    return 0;
}

size_t RemoteFileCache::getFileSegmentsNum() const
{
    return 0;
}

bool RemoteFileCache::tryReserve(const Key &, size_t, size_t, std::lock_guard<std::mutex> &)
{
    /// throw Exception(ErrorCodes::LOGICAL_ERROR, "Reserve cannot be call in remote file cache");
    /// always return true for test.
    return true;
}

void RemoteFileCache::remove(Key, size_t, std::lock_guard<std::mutex> &, std::lock_guard<std::mutex> &)
{
}

bool RemoteFileCache::isLastFileSegmentHolder(const Key &, size_t, std::lock_guard<std::mutex> &, std::lock_guard<std::mutex> &)
{
    return true;
}

void RemoteFileCache::reduceSizeToDownloaded(const Key &, size_t, std::lock_guard<std::mutex> &, std::lock_guard<std::mutex> &)
{
}

RemoteFileCache::CacheLogEntry::CacheLogEntry(const Key & key_, size_t offset_, size_t size_)
    : key(key_), offset(offset_), size(size_), used_time(time_in_microseconds(std::chrono::system_clock::now()))
{
}

};
