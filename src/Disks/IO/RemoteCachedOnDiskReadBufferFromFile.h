#pragma once

#include <algorithm>
#include <Connector/Connector.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Interpreters/FilesystemCacheLog.h>
#include <Common/logger_useful.h>


namespace CurrentMetrics
{
extern const Metric FilesystemCacheReadBuffers;
}

namespace DB
{

class RemoteCachedOnDiskReadBufferFromFile : public ReadBufferFromFileBase
{
public:
    using ImplementationBufferPtr = std::shared_ptr<ReadBufferFromFileBase>;
    using ImplementationBufferCreator = std::function<ImplementationBufferPtr()>;

    struct ReadSourceFromFile
    {
        int fd;
        uint64_t offset;
        uint64_t has_read = 0;
        mq_cache::QueryObjectSegment segment;

        ReadSourceFromFile(mq_cache::QueryObjectSegment & segment_) : offset(segment_.offset), segment(segment_)
        {
            fd = ::open(segment.cache_path.c_str(), O_RDONLY);
            assert(fd > 0);
            lseek(fd, offset, SEEK_SET);
        }

        ~ReadSourceFromFile()
        {
            close(fd);
        }

        /// All data has been read, return true;
        bool isEnd() { return has_read == segment.size; }

        uint64_t read(char * to, size_t size)
        {
            auto can_read_byes = std::min(size, segment.size - has_read);
            auto read_bytes = ::read(fd, to, can_read_byes);
            has_read += read_bytes;
            offset += read_bytes;
            return read_bytes;
        }
    };

    using ReadSourceFromFilePtr = std::shared_ptr<ReadSourceFromFile>;

    RemoteCachedOnDiskReadBufferFromFile(
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
        std::optional<size_t> read_until_position_ = std::nullopt);

    ~RemoteCachedOnDiskReadBufferFromFile() override;

    bool nextImpl() override;

    off_t seek(off_t off, int whence) override;

    off_t getPosition() override;

    size_t getFileOffsetOfBufferEnd() const override
    {
        return file_offset_of_buffer_end;
    }

    String getInfoForLog() override;

    void setReadUntilPosition(size_t position) override;

    void setReadUntilEnd() override;

    String getFileName() const override { return source_file_path; }

private:
    void initialize(size_t offset, size_t size);

    size_t getTotalSizeToRead() const;

    bool nextImplStep();

    void assertReadCacheIsCorrect(const char * s1, const char * s2, size_t size);

    Poco::Logger * log;
    FileCache::Key cache_key;
    String source_file_path;

    FileCachePtr cache;

    std::shared_ptr<mq_cache::MQCacheConnector> connector;
    mq_cache::QueryObjectSegments segments;
    ReadSourceFromFilePtr read_source_from_cache;

    ReadSettings settings;

    size_t read_until_position;
    size_t file_offset_of_buffer_end = 0;
    size_t bytes_to_predownload = 0;

    ImplementationBufferCreator implementation_buffer_creator;

    /// Remote read buffer, which can only be owned by current buffer.
    FileSegment::RemoteFileReaderPtr remote_file_reader;

    ImplementationBufferPtr implementation_buffer;
    bool initialized = false;

    size_t first_offset = 0;
    String nextimpl_step_log_info;
    String last_caller_id;

    String query_id;
    bool enable_logging = false;
    String current_buffer_id;

    bool allow_seeks_after_first_read;
    [[maybe_unused]] bool use_external_buffer;
    bool is_persistent;
    bool read_from_cache = false;
    uint64_t working_segment_pos = 0;
};

}
