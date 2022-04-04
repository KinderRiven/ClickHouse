#pragma once

#include <Disks/DiskLocal.h>
#include <Disks/IDisk.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>
#include <base/logger_useful.h>
#include "SliceManagement.h"

namespace DB
{
///
/// SliceReadBuffer holds the fragment information of the original file, the disk
/// that saves the local cache, the remotedisk that saves the fragment cache, and
/// the remotedisk that saves the complete data.
///
class SliceReadBuffer : public ReadBufferFromFileBase
{
public:
    struct Slice
    {
    public:
        Slice() = default;

        Slice(size_t num_blocks_, size_t uncompressed_size_, size_t compressed_size_, size_t offset_in_compressed_file_)
            : num_blocks(num_blocks_)
            , uncompressed_size(uncompressed_size_)
            , compressed_size(compressed_size_)
            , offset_in_compressed_file(offset_in_compressed_file_)
        {
        }

    public:
        size_t num_blocks = 0;
        size_t uncompressed_size = 0;
        size_t compressed_size = 0;
        size_t offset_in_compressed_file = 0;
    };

public:
    ///
    /// [1] slice_file: File for saving slice information.
    /// [2] local_cache: Local disk, which can be used to cache fragment data and read fragment cache from it.
    /// [3] remote_cache: Remote cache, which has not been implemented yet.
    /// [4] remote_file: Remote data file. When both local cache and remote cache miss, you need to read data fragments from it.
    ///
    SliceReadBuffer(
        std::unique_ptr<ReadBufferFromFileBase> slice_file,
        std::shared_ptr<DiskLocal> local_cache,
        std::shared_ptr<IDisk> remote_cache,
        std::unique_ptr<ReadBufferFromFileBase> remote_file,
        const ReadSettings & settings,
        std::optional<size_t> size);

    ~SliceReadBuffer();

    std::string getFileName() const override { return remote_data_file->getFileName(); }

    off_t getPosition() override;

    off_t seek(off_t off, int whence) override;

    bool nextImpl() override;

private:
    String getRemoteSliceName(const String & path, int slice_id);

    String getLocalSlicePath(const String & path, int slice_id);

    int getSliceFromOffset(off_t off);

    off_t switchToSlice(int slice_id, off_t off);

    bool isTemp(const String & local_path);

    void tryToPrefetch(const String & path, int slice_id);

public:
    void downloadSliceFile(const String & path, int slice_id);

    void uploadSliceFile(const String & local_path, const String & remote_path);


private:
    int current_slice = -1;

    off_t offset_in_compressed_file = 0;

    std::unique_ptr<ReadBufferFromFileBase> slice_file;

    std::shared_ptr<DiskLocal> local_cache;

    std::shared_ptr<IDisk> remote_cache;

    std::unique_ptr<ReadBufferFromFileBase> remote_data_file;

    std::vector<Slice> vec_slice;

    std::unique_ptr<ReadBufferFromFileBase> current_slice_file;

    ReadSettings read_settings;

    std::optional<size_t> read_size;

    Poco::Logger * trace_log = &Poco::Logger::get("[slice_read]");
};
};