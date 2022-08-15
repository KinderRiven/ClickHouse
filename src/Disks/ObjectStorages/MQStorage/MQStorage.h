#pragma once
#include <memory>
#include <Core/UUID.h>
#include <Disks/IDisk.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Disks/ObjectStorages/S3/S3Capabilities.h>
#include <Disks/ObjectStorages/S3/S3ObjectStorage.h>
#include <Storages/StorageS3Settings.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/HeadObjectResult.h>
#include <aws/s3/model/ListObjectsV2Result.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/MultiVersion.h>
#include <Common/config.h>
#include <Common/logger_useful.h>

namespace DB
{

class MQStorage : public IObjectStorage
{
public:
    MQStorage(
        std::unique_ptr<Aws::S3::S3Client> && client_,
        std::unique_ptr<S3ObjectStorageSettings> && s3_settings_,
        String version_id_,
        const S3Capabilities & s3_capabilities_,
        String bucket_,
        FileCachePtr cache_)
        : s3_storage(
            std::make_unique<S3ObjectStorage>(std::move(client_), std::move(s3_settings_), version_id_, s3_capabilities_, bucket_, cache_))
    {
        LOG_INFO(log, "Create MQStorage Succeeed!");
    }

    std::string getName() const override { return "MQStorage"; }

    bool exists(const StoredObject & object) const override;

    std::unique_ptr<ReadBufferFromFileBase> readObject( /// NOLINT
        const StoredObject & object,
        const ReadSettings & read_settings = ReadSettings{},
        std::optional<size_t> read_hint = {},
        std::optional<size_t> file_size = {}) const override;

    std::unique_ptr<ReadBufferFromFileBase> readObjects( /// NOLINT
        const StoredObjects & objects,
        const ReadSettings & read_settings = ReadSettings{},
        std::optional<size_t> read_hint = {},
        std::optional<size_t> file_size = {}) const override;

    /// Open the file for write and return WriteBufferFromFileBase object.
    std::unique_ptr<WriteBufferFromFileBase> writeObject( /// NOLINT
        const StoredObject & object,
        WriteMode mode,
        std::optional<ObjectAttributes> attributes = {},
        FinalizeCallback && finalize_callback = {},
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        const WriteSettings & write_settings = {}) override;

    void listPrefix(const std::string & path, RelativePathsWithSize & children) const override;

    /// Remove file. Throws exception if file doesn't exists or it's a directory.
    void removeObject(const StoredObject & object) override;

    void removeObjects(const StoredObjects & objects) override;

    void removeObjectIfExists(const StoredObject & object) override;

    void removeObjectsIfExist(const StoredObjects & objects) override;

    ObjectMetadata getObjectMetadata(const std::string & path) const override;

    void setCapabilitiesSupportBatchDelete(bool value) { s3_storage->setCapabilitiesSupportBatchDelete(value); }

    void copyObject( /// NOLINT
        const StoredObject & object_from,
        const StoredObject & object_to,
        std::optional<ObjectAttributes> object_to_attributes = {}) override;

    void shutdown() override;

    void startup() override;

    void applyNewSettings(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, ContextPtr context) override;

    String getObjectsNamespace() const override { return ""; }

    std::unique_ptr<IObjectStorage> cloneObjectStorage(
        const std::string & new_namespace,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        ContextPtr context) override;

    std::string generateBlobNameForPath(const std::string & path) override;

    bool isRemote() const override { return true; }

private:
    std::unique_ptr<S3ObjectStorage> s3_storage;
    Poco::Logger * log = &Poco::Logger::get("MQStorage");
};

}
