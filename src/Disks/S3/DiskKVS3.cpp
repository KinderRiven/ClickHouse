#include "DiskKVS3.h"
#include <IO/ReadBufferFromS3.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/SeekAvoidingReadBuffer.h>
#include <IO/WriteBufferFromFileDecorator.h>
#include <IO/WriteBufferFromS3.h>
#include <IO/WriteHelpers.h>

#include <aws/s3/model/HeadObjectResult.h>
#include <aws/s3/model/HeadObjectRequest.h>

namespace DB
{

DiskKVS3::DiskKVS3(
    String name_, String bucket_, String s3_root_path_, ContextPtr context_, SettingsPtr settings_, GetDiskSettings settings_getter_)
    : name(std::move(name_))
    , s3_root_path(std::move(s3_root_path_))
    , bucket(std::move(bucket_))
    , current_settings(std::move(settings_))
    , settings_getter(settings_getter_)
    , context(context_)
{
}

bool DiskKVS3::exists(const String & key_) const
{
    auto settings = current_settings.get();
    auto client_ptr = settings->client;
    String key = s3_root_path + key_;

    Aws::S3::Model::HeadObjectRequest req;
    req.SetBucket(bucket);
    req.SetKey(key);

    auto result = client_ptr->HeadObject(req);
    if (result.IsSuccess())
    {
        return true;
    }
    else
    {
        return false;
    }
}

size_t DiskKVS3::getFileSize(const String &) const
{
    return 0;
}

void DiskKVS3::createFile(const String &)
{
}

std::unique_ptr<ReadBufferFromFileBase>
DiskKVS3::readFile(const String & key, const ReadSettings & read_settings, std::optional<size_t>) const
{
    auto settings = current_settings.get();
    auto s3_buffer = std::make_unique<ReadBufferFromS3>(
        settings->client, bucket, s3_root_path + key, settings->s3_max_single_read_retries, read_settings);
    return std::make_unique<ReadBufferFromFileDecorator>(std::move(s3_buffer));
}

std::unique_ptr<WriteBufferFromFileBase> DiskKVS3::writeFile(const String & key, size_t buf_size, WriteMode)
{
    auto settings = current_settings.get();
    auto s3_buffer = std::make_unique<WriteBufferFromS3>(
        settings->client,
        bucket,
        s3_root_path + key,
        settings->s3_min_upload_part_size,
        settings->s3_max_single_part_upload_size,
        std::nullopt,
        buf_size);
    return std::make_unique<WriteBufferFromFileDecorator>(std::move(s3_buffer));
}

void DiskKVS3::removeFile(const String &)
{
}

void DiskKVS3::removeFileIfExists(const String &)
{
}

void DiskKVS3::shutdown()
{
    auto settings = current_settings.get();
    /// This call stops any next retry attempts for ongoing S3 requests.
    /// If S3 request is failed and the method below is executed S3 client immediately returns the last failed S3 request outcome.
    /// If S3 is healthy nothing wrong will be happened and S3 requests will be processed in a regular way without errors.
    /// This should significantly speed up shutdown process if S3 is unhealthy.
    settings->client->DisableRequestProcessing();
}

void DiskKVS3::startup()
{
    auto settings = current_settings.get();
    /// Need to be enabled if it was disabled during shutdown() call.
    settings->client->EnableRequestProcessing();
}

};
