#pragma once

#include <Disks/IDiskKV.h>
#include <Disks/S3/DiskS3.h>

namespace DB
{

class DiskKVS3 : public IDiskKV
{
public:
    using SettingsPtr = std::unique_ptr<DiskS3Settings>;
    using GetDiskSettings = std::function<SettingsPtr(const Poco::Util::AbstractConfiguration &, const String, ContextPtr)>;

public:
    DiskKVS3(String name, String bucket, String s3_root_path, ContextPtr context, SettingsPtr settings, GetDiskSettings settings_getter);

    const String & getName() const final override { return name; }

    bool exists(const String & key) const override;

    size_t getFileSize(const String & key) const override;

    void createFile(const String & key) override;

    std::unique_ptr<ReadBufferFromFileBase>
    readFile(const String & key, const ReadSettings & settings = ReadSettings{}, std::optional<size_t> size = {}) const override;

    std::unique_ptr<WriteBufferFromFileBase>
    writeFile(const String & key, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE, WriteMode mode = WriteMode::Rewrite) override;

    void removeFile(const String & key) override;

    void removeFileIfExists(const String & key) override;

    DiskType getType() const override { return DiskType::KV_S3; }

    void shutdown() override;

    void startup() override;

private:
    const String name;

    const String s3_root_path;

    const String bucket;

    MultiVersion<DiskS3Settings> current_settings;

    /// Gets disk settings from context.
    GetDiskSettings settings_getter;

    ContextPtr context;
};

};