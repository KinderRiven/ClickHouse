#include "RemoteCachedOnDiskReadBufferFromFile.h"

#include <algorithm>
#include <string.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/Context.h>
#include <base/scope_guard.h>
#include <Common/assert_cast.h>
#include <Common/getRandomASCIIString.h>
#include <Common/hex.h>

namespace ProfileEvents
{
extern const Event FileSegmentWaitReadBufferMicroseconds;
extern const Event FileSegmentReadMicroseconds;
extern const Event FileSegmentCacheWriteMicroseconds;
extern const Event FileSegmentPredownloadMicroseconds;
extern const Event FileSegmentUsedBytes;

extern const Event CachedReadBufferReadFromSourceMicroseconds;
extern const Event CachedReadBufferReadFromCacheMicroseconds;
extern const Event CachedReadBufferCacheWriteMicroseconds;
extern const Event CachedReadBufferReadFromSourceBytes;
extern const Event CachedReadBufferReadFromCacheBytes;
extern const Event CachedReadBufferCacheWriteBytes;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int CANNOT_USE_CACHE;
    extern const int LOGICAL_ERROR;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

RemoteCachedOnDiskReadBufferFromFile::RemoteCachedOnDiskReadBufferFromFile(
    const String & source_file_path_,
    const FileCache::Key & cache_key_,
    FileCachePtr cache_,
    std::shared_ptr<mq_cache::MQCacheConnector> connector_,
    ImplementationBufferCreator implementation_buffer_creator_,
    const ReadSettings & settings_,
    const String & query_id_,
    size_t file_size_,
    bool allow_seeks_after_first_read_,
    bool use_external_buffer_,
    std::optional<size_t> read_until_position_)
    : ReadBufferFromFileBase(settings_.remote_fs_buffer_size, nullptr, 0, file_size_)
#ifndef NDEBUG
    , log(&Poco::Logger::get("RemoteCachedOnDiskReadBufferFromFile(" + source_file_path_ + ")"))
#else
    , log(&Poco::Logger::get("RemoteCachedOnDiskReadBufferFromFile"))
#endif
    , cache_key(cache_key_)
    , source_file_path(source_file_path_)
    , cache(cache_)
    , connector(connector_)
    , settings(settings_)
    , read_until_position(read_until_position_ ? *read_until_position_ : file_size_)
    , implementation_buffer_creator(implementation_buffer_creator_)
    , query_id(query_id_)
    , enable_logging(!query_id.empty() && settings_.enable_filesystem_cache_log)
    , current_buffer_id(getRandomASCIIString(8))
    , allow_seeks_after_first_read(allow_seeks_after_first_read_)
    , use_external_buffer(use_external_buffer_)
    , is_persistent(settings_.is_file_cache_persistent)
{
    cache_key.toString();
    is_persistent = true;
    bytes_to_predownload = 0;
    allow_seeks_after_first_read = false;
    /// LOG_INFO(log, "[CREATE] name:{}, offset:{}, size:{}", source_file_path, read_until_position, getFileSize());
}

size_t RemoteCachedOnDiskReadBufferFromFile::getTotalSizeToRead() const
{
    /// Last position should be guaranteed to be set, as at least we always know file size.
    if (!read_until_position)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Last position was not set");

    /// On this level should be guaranteed that read size is non-zero.
    if (file_offset_of_buffer_end >= read_until_position)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Read boundaries mismatch. Expected {} < {}", file_offset_of_buffer_end, read_until_position);

    return read_until_position - file_offset_of_buffer_end;
}

void RemoteCachedOnDiskReadBufferFromFile::initialize(size_t offset, size_t size)
{
    if (initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Caching buffer already initialized");

    std::string endpoint = "https://shukai-clickhouse.s3.ap-southeast-1.amazonaws.com/";
    std::string source_path = source_file_path;
    segments = connector->queryObject(endpoint, source_path, offset, size);
    assert(segments.size() > 0);

    if (segments[0].state == "EMPTY")
    {
        remote_file_reader = implementation_buffer_creator();
        implementation_buffer = remote_file_reader;
        implementation_buffer->seek(offset, SEEK_SET);
        LOG_INFO(log, "[INIT] read name:{}, offset:{}, size:{} from S3Storage", source_file_path, offset, size);
        read_from_cache = false;
    }
    else
    {
        assert(segments[0].state == "DOWNLOADED");
        read_source_from_cache = std::make_shared<RemoteCachedOnDiskReadBufferFromFile::ReadSourceFromFile>(segments[working_segment_pos]);
        LOG_INFO(log, "[INIT] read name:{} from MQCache, segments size is {}", source_file_path, segments.size());
        read_from_cache = true;
    }
    initialized = true;
}

RemoteCachedOnDiskReadBufferFromFile::~RemoteCachedOnDiskReadBufferFromFile()
{
}

bool RemoteCachedOnDiskReadBufferFromFile::nextImpl()
{
    try
    {
        return nextImplStep();
    }
    catch (Exception & e)
    {
        e.addMessage("Cache info: {}", nextimpl_step_log_info);
        throw;
    }
}

bool RemoteCachedOnDiskReadBufferFromFile::nextImplStep()
{
    if (!initialized)
        initialize(file_offset_of_buffer_end, getTotalSizeToRead());
    assert(!internal_buffer.empty());

    bool result = false;
    if (read_from_cache)
    {
        if (read_source_from_cache->isEnd())
        {
            /// TODO get more segment.
            if (working_segment_pos == (segments.size() - 1))
            {
                return false;
            }
            else
            {
                working_segment_pos++;
                read_source_from_cache
                    = std::make_shared<RemoteCachedOnDiskReadBufferFromFile::ReadSourceFromFile>(segments[working_segment_pos]);
            }
        }
        /// TODO read from cache string
        size_t bytes_to_read = read_source_from_cache->read(internal_buffer.begin(), internal_buffer.size());
        assert(bytes_to_read);
        working_buffer = internal_buffer;
        working_buffer.resize(bytes_to_read); /// set working_buffer.end()
        file_offset_of_buffer_end += bytes_to_read;
        result = true;
    }
    else
    {
        swap(*implementation_buffer);
        result = implementation_buffer->next();
        swap(*implementation_buffer);
        file_offset_of_buffer_end += available();
    }
    /// LOG_INFO(log, "[nextImplStep] name:{}, buffer_end_offset:{}, read_until_position:{}",
    ///     source_file_path, file_offset_of_buffer_end, read_until_position);
    return result;
}

off_t RemoteCachedOnDiskReadBufferFromFile::seek(off_t offset, int)
{
    if (initialized && !allow_seeks_after_first_read)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Not allow seek after first read");

    first_offset = file_offset_of_buffer_end = offset;
    return offset;
}

void RemoteCachedOnDiskReadBufferFromFile::setReadUntilPosition(size_t position)
{
    read_until_position = position;
}

void RemoteCachedOnDiskReadBufferFromFile::setReadUntilEnd()
{
    setReadUntilPosition(getFileSize());
}

off_t RemoteCachedOnDiskReadBufferFromFile::getPosition()
{
    /// LOG_INFO(log, "name:{}, getPosition:{}", getFileName(), file_offset_of_buffer_end - available());
    return file_offset_of_buffer_end - available();
}

String RemoteCachedOnDiskReadBufferFromFile::getInfoForLog()
{
    String result = "Noting to print for remote_cached_buffer";
    return result;
}

}
