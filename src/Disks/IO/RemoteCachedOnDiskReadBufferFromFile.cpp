#include "RemoteCachedOnDiskReadBufferFromFile.h"

#include <Disks/IO/createReadBufferFromFileBase.h>
#include <IO/ReadBufferFromFile.h>
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
    first_offset = 0;
    allow_seeks_after_first_read = true;
    remote_file_reader = implementation_buffer_creator();
    LOG_INFO(
        log,
        "Need to read name:{}, size1:{}, size2:{}, allow_seek:{}",
        remote_file_reader->getFileName(),
        remote_file_reader->getFileSize(),
        file_size_,
        allow_seeks_after_first_read);
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

    if (connector)
    {
        std::string endpoint = "https://shukai-clickhouse.s3.ap-southeast-1.amazonaws.com/";
        std::string source_path = source_file_path;
        connector->queryObject(endpoint, source_path, offset, size);
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
        LOG_INFO(
            log,
            "nextImpl:{}, pos1:{}, pos2:{}, file_size:{}",
            remote_file_reader->getFileName(),
            getPosition(),
            remote_file_reader->getPosition(),
            remote_file_reader->getFileSize());
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

    LOG_INFO(
        log,
        "start_nextImpl:{}, position:{}/{}, available:{}/{}",
        getFileName(),
        remote_file_reader->getPosition(), /// 0
        getPosition(), /// 0
        remote_file_reader->available(), /// 0
        available()); /// 0
    
    /// My buffer is empty, we need to fill it, swap it to remote_file_reader and fill it!
    /// My buffer is (0, 0)
    /// remote_file_reader buffer is (0, 0)
    swap(*remote_file_reader);
    /// My buffer is swap to remote_file_reader, it use next to fill it!
    auto result = remote_file_reader->next();
    /// My buffer has been filled, swap to myself.
    swap(*remote_file_reader);
    /// My buffer is (0, 83184), pos is 0
    /// remote_file_reader buffer is (0, 0), pos is 0

    LOG_INFO(
        log,
        "end_nextImpl:{}, position:{}/{}, available:{}/{}",
        getFileName(),
        remote_file_reader->getPosition(), /// 83184, because s3 read buffer getPosition() is offset - available()
        getPosition(), /// 0
        remote_file_reader->available(), /// 0
        available()); /// 83184
    return result;
}

off_t RemoteCachedOnDiskReadBufferFromFile::seek(off_t offset, int whence)
{
    LOG_INFO(
        log,
        "start_seek:{}, pos1:{}, pos2:{}, offset:{}, file_size:{}, initializate:{}, available1:{}, available2:{}",
        remote_file_reader->getFileName(),
        getPosition(),
        remote_file_reader->getPosition(),
        offset,
        remote_file_reader->getFileSize(),
        initialized,
        available(),
        remote_file_reader->available());

    swap(*remote_file_reader);
    LOG_INFO(log, "start_seek:{}, available:{}", remote_file_reader->getFileName(), remote_file_reader->available());
    auto result = remote_file_reader->seek(offset, whence);
    LOG_INFO(log, "end_seek:{}, available:{}", remote_file_reader->getFileName(), remote_file_reader->available());
    swap(*remote_file_reader);
    
    LOG_INFO(
        log,
        "end_seek:{}, pos1:{}, pos2:{}, offset:{}, file_size:{}, initializate:{}, available1:{}, available2:{}",
        remote_file_reader->getFileName(),
        getPosition(),
        remote_file_reader->getPosition(),
        offset,
        remote_file_reader->getFileSize(),
        initialized,
        available(),
        remote_file_reader->available());
    return result;
}

void RemoteCachedOnDiskReadBufferFromFile::setReadUntilPosition(size_t position)
{
    LOG_INFO(
        log,
        "setReadUntilPosition:{}, pos1:{}, pos2:{}, position:{}, file_size:{}, initializate:{}",
        remote_file_reader->getFileName(),
        getPosition(),
        remote_file_reader->getPosition(),
        position,
        remote_file_reader->getFileSize(),
        initialized);

    swap(*remote_file_reader);
    LOG_INFO(log, "setReadUntilPosition:{}, available:{}", remote_file_reader->getFileName(), remote_file_reader->available());
    remote_file_reader->setReadUntilPosition(position);
    swap(*remote_file_reader);
}

void RemoteCachedOnDiskReadBufferFromFile::setReadUntilEnd()
{
    LOG_INFO(
        log,
        "setReadUntilEnd:{}, pos1:{}, pos2:{}, file_size:{}, initializate:{}",
        remote_file_reader->getFileName(),
        getPosition(),
        remote_file_reader->getPosition(),
        remote_file_reader->getFileSize(),
        initialized);

    swap(*remote_file_reader);
    LOG_INFO(log, "setReadUntilEnd:{}, available:{}", remote_file_reader->getFileName(), remote_file_reader->available());
    remote_file_reader->setReadUntilEnd();
    swap(*remote_file_reader);
}

off_t RemoteCachedOnDiskReadBufferFromFile::getPosition()
{
    swap(*remote_file_reader);
    auto result = remote_file_reader->getPosition();
    swap(*remote_file_reader);
    return result;
}

String RemoteCachedOnDiskReadBufferFromFile::getInfoForLog()
{
    String log_str = "NOTHING TO PRINT";
    return log_str;
}

}
