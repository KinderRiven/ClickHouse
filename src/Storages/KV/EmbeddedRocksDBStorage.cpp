#include <Storages/KV/EmbeddedRocksDBStorage.h>
#include <rocksdb/convenience.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/db_ttl.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ROCKSDB_ERROR;
}

using RocksDBOptions = std::unordered_map<std::string, std::string>;

static RocksDBOptions getOptionsFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & path)
{
    RocksDBOptions options;

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(path, keys);

    for (const auto & key : keys)
    {
        const String key_path = path + "." + key;
        options[key] = config.getString(key_path);
    }

    return options;
}

static IEmbeddedKeyValueStorage::Status::Code convertStatusCode(rocksdb::Status & status)
{
    switch (status.code())
    {
        case rocksdb::Status::Code::kOk:
            return IEmbeddedKeyValueStorage::Status::Code::kOk;
        case rocksdb::Status::Code::kNotFound:
            return IEmbeddedKeyValueStorage::Status::Code::kNotFound;
        case rocksdb::Status::Code::kCorruption:
            return IEmbeddedKeyValueStorage::Status::Code::kCorruption;
        case rocksdb::Status::Code::kNotSupported:
            return IEmbeddedKeyValueStorage::Status::Code::kNotSupported;
        case rocksdb::Status::Code::kInvalidArgument:
            return IEmbeddedKeyValueStorage::Status::Code::kInvalidArgument;
        case rocksdb::Status::Code::kIOError:
            return IEmbeddedKeyValueStorage::Status::Code::kIOError;
        case rocksdb::Status::Code::kMergeInProgress:
            return IEmbeddedKeyValueStorage::Status::Code::kMergeInProgress;
        case rocksdb::Status::Code::kIncomplete:
            return IEmbeddedKeyValueStorage::Status::Code::kIncomplete;
        case rocksdb::Status::Code::kShutdownInProgress:
            return IEmbeddedKeyValueStorage::Status::Code::kShutdownInProgress;
        case rocksdb::Status::Code::kTimedOut:
            return IEmbeddedKeyValueStorage::Status::Code::kTimedOut;
        case rocksdb::Status::Code::kAborted:
            return IEmbeddedKeyValueStorage::Status::Code::kAborted;
        case rocksdb::Status::Code::kBusy:
            return IEmbeddedKeyValueStorage::Status::Code::kBusy;
        case rocksdb::Status::Code::kExpired:
            return IEmbeddedKeyValueStorage::Status::Code::kExpired;
        case rocksdb::Status::Code::kTryAgain:
            return IEmbeddedKeyValueStorage::Status::Code::kTryAgain;
        case rocksdb::Status::Code::kCompactionTooLarge:
            return IEmbeddedKeyValueStorage::Status::Code::kCompactionTooLarge;
        case rocksdb::Status::Code::kColumnFamilyDropped:
            return IEmbeddedKeyValueStorage::Status::Code::kColumnFamilyDropped;
        case rocksdb::Status::Code::kMaxCode:
            return IEmbeddedKeyValueStorage::Status::Code::kMaxCode;
    }
    UNREACHABLE();
}

String EmbeddedRocksDBStorage::ReadIterator::key()
{
    return iterator->key().ToString();
}

String EmbeddedRocksDBStorage::ReadIterator::value()
{
    return iterator->value().ToString();
}

void EmbeddedRocksDBStorage::ReadIterator::seekToFirst()
{
    iterator->SeekToFirst();
}

void EmbeddedRocksDBStorage::ReadIterator::next()
{
    iterator->Next();
}

bool EmbeddedRocksDBStorage::ReadIterator::valid()
{
    return iterator->Valid();
}

IEmbeddedKeyValueStorage::Status EmbeddedRocksDBStorage::WriteIterator::put(String & key, String & value)
{
    auto status = batch.Put(key, value);
    return IEmbeddedKeyValueStorage::Status(convertStatusCode(status));
}

IEmbeddedKeyValueStorage::Status EmbeddedRocksDBStorage::WriteIterator::remove(String & key)
{
    auto status = batch.Delete(key);
    return IEmbeddedKeyValueStorage::Status(convertStatusCode(status));
}

IEmbeddedKeyValueStorage::Status EmbeddedRocksDBStorage::WriteIterator::commit()
{
    auto status = rocksdb_ptr->Write(rocksdb::WriteOptions(), &batch);
    return IEmbeddedKeyValueStorage::Status(convertStatusCode(status));
}

EmbeddedRocksDBStorage::EmbeddedRocksDBStorage()
{
}

void EmbeddedRocksDBStorage::truncate(EmbeddedKeyValueStorageOptions & options)
{
    std::lock_guard lock(rocksdb_ptr_mx);
    rocksdb_ptr->Close();
    rocksdb_ptr = nullptr;

    fs::remove_all(options.rocksdb_dir);
    fs::create_directories(options.rocksdb_dir);
    initDB(options);
}

void EmbeddedRocksDBStorage::initDB(EmbeddedKeyValueStorageOptions & options)
{
    rocksdb::Status status;
    rocksdb::Options base;

    base.create_if_missing = true;
    base.compression = rocksdb::CompressionType::kZSTD;
    base.statistics = rocksdb::CreateDBStatistics();
    /// It is too verbose by default, and in fact we don't care about rocksdb logs at all.
    base.info_log_level = rocksdb::ERROR_LEVEL;

    rocksdb::Options merged = base;

    const auto & config = options.config;
    if (config.has("rocksdb.options"))
    {
        auto config_options = getOptionsFromConfig(config, "rocksdb.options");
        status = rocksdb::GetDBOptionsFromMap(merged, config_options, &merged);
        if (!status.ok())
        {
            throw Exception(
                ErrorCodes::ROCKSDB_ERROR,
                "Fail to merge rocksdb options from 'rocksdb.options' at: {}: {}",
                options.rocksdb_dir,
                status.ToString());
        }
    }
    if (config.has("rocksdb.column_family_options"))
    {
        auto column_family_options = getOptionsFromConfig(config, "rocksdb.column_family_options");
        status = rocksdb::GetColumnFamilyOptionsFromMap(merged, column_family_options, &merged);
        if (!status.ok())
        {
            throw Exception(
                ErrorCodes::ROCKSDB_ERROR,
                "Fail to merge rocksdb options from 'rocksdb.options' at: {}: {}",
                options.rocksdb_dir,
                status.ToString());
        }
    }

    if (config.has("rocksdb.tables"))
    {
        auto table_name = options.table_name;

        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys("rocksdb.tables", keys);

        for (const auto & key : keys)
        {
            const String key_prefix = "rocksdb.tables." + key;
            if (config.getString(key_prefix + ".name") != table_name)
                continue;

            String config_key = key_prefix + ".options";
            if (config.has(config_key))
            {
                auto table_config_options = getOptionsFromConfig(config, config_key);
                status = rocksdb::GetDBOptionsFromMap(merged, table_config_options, &merged);
                if (!status.ok())
                {
                    throw Exception(
                        ErrorCodes::ROCKSDB_ERROR,
                        "Fail to merge rocksdb options from '{}' at: {}: {}",
                        config_key,
                        options.rocksdb_dir,
                        status.ToString());
                }
            }

            config_key = key_prefix + ".column_family_options";
            if (config.has(config_key))
            {
                auto table_column_family_options = getOptionsFromConfig(config, config_key);
                status = rocksdb::GetColumnFamilyOptionsFromMap(merged, table_column_family_options, &merged);
                if (!status.ok())
                {
                    throw Exception(
                        ErrorCodes::ROCKSDB_ERROR,
                        "Fail to merge rocksdb options from '{}' at: {}: {}",
                        config_key,
                        options.rocksdb_dir,
                        status.ToString());
                }
            }
        }
    }

    if (options.ttl > 0)
    {
        rocksdb::DBWithTTL * db;
        status = rocksdb::DBWithTTL::Open(merged, options.rocksdb_dir, &db, options.ttl, options.read_only);
        if (!status.ok())
        {
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to open rocksdb path at: {}: {}", options.rocksdb_dir, status.ToString());
        }
        rocksdb_ptr = std::unique_ptr<rocksdb::DBWithTTL>(db);
    }
    else
    {
        rocksdb::DB * db;
        if (options.read_only)
        {
            status = rocksdb::DB::OpenForReadOnly(merged, options.rocksdb_dir, &db);
        }
        else
        {
            status = rocksdb::DB::Open(merged, options.rocksdb_dir, &db);
        }
        if (!status.ok())
        {
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to open rocksdb path at: {}: {}", options.rocksdb_dir, status.ToString());
        }
        rocksdb_ptr = std::unique_ptr<rocksdb::DB>(db);
    }
}

IEmbeddedKeyValueStorage::Reader EmbeddedRocksDBStorage::getReader() const
{
    return std::make_unique<EmbeddedRocksDBStorage::ReadIterator>(
        std::unique_ptr<rocksdb::Iterator>(rocksdb_ptr->NewIterator(rocksdb::ReadOptions())));
}

IEmbeddedKeyValueStorage::Writer EmbeddedRocksDBStorage::getWriter()
{
    return std::make_unique<EmbeddedRocksDBStorage::WriteIterator>(rocksdb_ptr);
}

};
