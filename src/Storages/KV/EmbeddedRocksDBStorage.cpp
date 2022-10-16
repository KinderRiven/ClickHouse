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

bool EmbeddedRocksDBStorage::WriteIterator::put(String & key, String & value)
{
    rocksdb::Status status = batch.Put(key, value);
    return status.ok();
}

bool EmbeddedRocksDBStorage::WriteIterator::commit()
{
    rocksdb_ptr->Write(rocksdb::WriteOptions(), &batch);
    return true;
}

EmbeddedRocksDBStorage::EmbeddedRocksDBStorage()
{
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

    const auto & config = options.context->getConfigRef();
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
        auto table_name = options.storage_id.getTableName();

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
