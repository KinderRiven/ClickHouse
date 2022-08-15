#include <Disks/ObjectStorages/MQStorage/MQStorage.h>

#include <Disks/IO/AsynchronousReadIndirectBufferFromRemoteFS.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/IO/ReadIndirectBufferFromRemoteFS.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <Disks/IO/WriteIndirectBufferFromRemoteFS.h>
#include <Disks/ObjectStorages/DiskObjectStorageCommon.h>
#include <Disks/ObjectStorages/S3/diskSettings.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/SeekAvoidingReadBuffer.h>
#include <IO/WriteBufferFromS3.h>
#include <Interpreters/threadPoolCallbackRunner.h>

#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/UploadPartCopyRequest.h>

#include <Common/FileCacheFactory.h>
#include <Common/IFileCache.h>
#include <Common/MultiVersion.h>
#include <Common/getRandomASCIIString.h>
#include <Common/logger_useful.h>

namespace DB
{

bool MQStorage::exists(const StoredObject & object) const
{
    return s3_storage->exists(object);
}

std::unique_ptr<ReadBufferFromFileBase> MQStorage::readObject( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    std::optional<size_t> read_hint,
    std::optional<size_t> file_size) const
{
    LOG_INFO(log, "ObjectPath:{}, ObjectSize:{}", object.absolute_path, object.bytes_size);
    return s3_storage->readObject(object, read_settings, read_hint, file_size);
}

std::unique_ptr<ReadBufferFromFileBase> MQStorage::readObjects( /// NOLINT
    const StoredObjects & objects,
    const ReadSettings & read_settings,
    std::optional<size_t> read_hint,
    std::optional<size_t> file_size) const
{
    for (const auto & object : objects)
    {
        LOG_INFO(log, "ObjectPath:{}, ObjectSize:{}", object.absolute_path, object.bytes_size);
    }
    return s3_storage->readObjects(objects, read_settings, read_hint, file_size);
}

/// Open the file for write and return WriteBufferFromFileBase object.
std::unique_ptr<WriteBufferFromFileBase> MQStorage::writeObject( /// NOLINT
    const StoredObject & object,
    WriteMode mode,
    std::optional<ObjectAttributes> attributes,
    FinalizeCallback && finalize_callback,
    size_t buf_size,
    const WriteSettings & write_settings)
{
    return s3_storage->writeObject(object, mode, attributes, std::move(finalize_callback), buf_size, write_settings);
}

void MQStorage::listPrefix(const std::string & path, RelativePathsWithSize & children) const
{
    s3_storage->listPrefix(path, children);
}

/// Remove file. Throws exception if file doesn't exists or it's a directory.
void MQStorage::removeObject(const StoredObject & object)
{
    s3_storage->removeObject(object);
}

void MQStorage::removeObjects(const StoredObjects & objects)
{
    s3_storage->removeObjects(objects);
}

void MQStorage::removeObjectIfExists(const StoredObject & object)
{
    s3_storage->removeObjectIfExists(object);
}

void MQStorage::removeObjectsIfExist(const StoredObjects & objects)
{
    s3_storage->removeObjectsIfExist(objects);
}

ObjectMetadata MQStorage::getObjectMetadata(const std::string & path) const
{
    return s3_storage->getObjectMetadata(path);
}

void MQStorage::copyObject( /// NOLINT
    const StoredObject & object_from,
    const StoredObject & object_to,
    std::optional<ObjectAttributes> object_to_attributes)
{
    return s3_storage->copyObject(object_from, object_to, object_to_attributes);
}

void MQStorage::shutdown()
{
    s3_storage->shutdown();
}

void MQStorage::startup()
{
    s3_storage->startup();
}

void MQStorage::applyNewSettings(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, ContextPtr context)
{
    s3_storage->applyNewSettings(config, config_prefix, context);
}

std::unique_ptr<IObjectStorage> MQStorage::cloneObjectStorage(
    const std::string & new_namespace,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    ContextPtr context)
{
    return s3_storage->cloneObjectStorage(new_namespace, config, config_prefix, context);
}

std::string MQStorage::generateBlobNameForPath(const std::string & path)
{
    return s3_storage->generateBlobNameForPath(path);
}
}
