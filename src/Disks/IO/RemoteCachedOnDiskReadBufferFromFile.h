#pragma once

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
        return remote_file_reader->getFileOffsetOfBufferEnd();
    }

    String getInfoForLog() override;

    void setReadUntilPosition(size_t position) override;

    void setReadUntilEnd() override;

    String getFileName() const override { return source_file_path; }

private:
    void initialize(size_t offset, size_t size);

    size_t getTotalSizeToRead() const;

    bool nextImplStep();

    Poco::Logger * log;
    FileCache::Key cache_key;
    String source_file_path;

    FileCachePtr cache;
    std::shared_ptr<mq_cache::MQCacheConnector> connector;
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
};

}
