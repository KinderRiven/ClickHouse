#include <filesystem>
#include <shared_mutex>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/Set.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/convertFieldToType.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/KV/RocksDB/StorageEmbeddedRocksDB.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageFactory.h>
#include <base/sort.h>
#include <Poco/Logger.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/logger_useful.h>

#include <rocksdb/convenience.h>
#include <rocksdb/db.h>
#include <rocksdb/table.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ROCKSDB_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNSUPPORTED_METHOD;
}

using RocksDBOptions = std::unordered_map<std::string, std::string>;

StorageEmbeddedRocksDBReader::StorageEmbeddedRocksDBReader(StorageEmbeddedRocksDB & rocksdb_) : rocksdb(rocksdb_)
{
    iterator = std::unique_ptr<rocksdb::Iterator>(rocksdb_.rocksdb_ptr->NewIterator(rocksdb::ReadOptions()));
    iterator->SeekToFirst();
}

StorageEmbeddedRocksDBReader::~StorageEmbeddedRocksDBReader() = default;

StorageEmbeddedRocksDBReader::Status StorageEmbeddedRocksDBReader::readRow(String & key, String & value) const
{
    if (!iterator->Valid())
        return StorageEmbeddedRocksDBReader::Status::ERROR;

    key = iterator->key().ToString();
    value = iterator->value().ToString();
    iterator->Next();
    return StorageEmbeddedRocksDBReader::Status::FOUND;
}

std::vector<StorageEmbeddedRocksDBReader::Status>
StorageEmbeddedRocksDBReader::readRows(std::vector<String> &, std::vector<String> &, size_t) const
{
    throw Exception(" StorageEmbeddedRocksDB not support 'readRows'", ErrorCodes::UNSUPPORTED_METHOD);
}

StorageEmbeddedRocksDBReader::Status StorageEmbeddedRocksDBReader::readRowWithKey(const String &, String &) const
{
    throw Exception(" StorageEmbeddedRocksDB not support 'readRowWithKey'", ErrorCodes::UNSUPPORTED_METHOD);
}

std::vector<StorageEmbeddedRocksDBReader::Status>
StorageEmbeddedRocksDBReader::readRowsWithKeys(const std::vector<String> & keys, std::vector<String> & values) const
{
    std::shared_lock<std::shared_mutex> lock(rocksdb.rocksdb_ptr_mx);
    if (!rocksdb.rocksdb_ptr)
        return {};

    String value;
    std::vector<StorageEmbeddedRocksDBReader::Status> result;
    for (const auto & key : keys)
    {
        auto status = rocksdb.rocksdb_ptr->Get(rocksdb::ReadOptions(), key, &value);
        if (status.ok())
        {
            values.emplace_back(value);
            result.emplace_back(StorageEmbeddedRocksDBReader::Status::FOUND);
        }
        else
        {
            values.emplace_back("");
            result.emplace_back(StorageEmbeddedRocksDBReader::Status::NOT_FOUND);
        }
    }
    return result;
}

bool StorageEmbeddedRocksDBReader::Valid() const
{
    return iterator->Valid();
}

void StorageEmbeddedRocksDBReader::SeekToFirst()
{
    iterator->SeekToFirst();
}

void StorageEmbeddedRocksDBReader::Seek(String & key)
{
    iterator->Seek(key);
}

void StorageEmbeddedRocksDBReader::Next()
{
    iterator->Next();
}

String StorageEmbeddedRocksDBReader::key() const
{
    return iterator->key().ToString();
}

String StorageEmbeddedRocksDBReader::value() const
{
    return iterator->value().ToString();
}

StorageEmbeddedRocksDBWriter::StorageEmbeddedRocksDBWriter(StorageEmbeddedRocksDB & rocksdb_) : rocksdb(rocksdb_)
{
}

StorageEmbeddedRocksDBWriter::~StorageEmbeddedRocksDBWriter() = default;

StorageEmbeddedRocksDBWriter::Status StorageEmbeddedRocksDBWriter::Put(const String & key, const String & value)
{
    auto status = rocksdb.rocksdb_ptr->Put(rocksdb::WriteOptions(), key, value);
    return status.ok() ? StorageEmbeddedRocksDBWriter::Status::OK : StorageEmbeddedRocksDBWriter::Status::ERROR;
}

StorageEmbeddedRocksDBWriter::Status StorageEmbeddedRocksDBWriter::BatchPut(const std::vector<String> &, const std::vector<String> &)
{
    throw Exception(" StorageEmbeddedRocksDB not support 'BatchPut'", ErrorCodes::UNSUPPORTED_METHOD);
}

StorageEmbeddedRocksDBWriter::Status StorageEmbeddedRocksDBWriter::Add(const String & key, const String & value)
{
    auto status = rocksdb.rocksdb_ptr->Put(rocksdb::WriteOptions(), key, value);
    return status.ok() ? StorageEmbeddedRocksDBWriter::Status::OK : StorageEmbeddedRocksDBWriter::Status::ERROR;
}

StorageEmbeddedRocksDBWriter::Status StorageEmbeddedRocksDBWriter::Commit()
{
    return StorageEmbeddedRocksDBWriter::Status::OK;
}

static StoragePtr create(const StorageFactory::Arguments & args)
{
    // TODO custom RocksDBSettings, table function
    if (!args.engine_args.empty())
        throw Exception(
            "Engine " + args.engine_name + " doesn't support any arguments (" + toString(args.engine_args.size()) + " given)",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    StorageInMemoryMetadata metadata;
    metadata.setColumns(args.columns);
    metadata.setConstraints(args.constraints);

    if (!args.storage_def->primary_key)
        throw Exception("StorageEmbeddedRocksDB must require one column in primary key", ErrorCodes::BAD_ARGUMENTS);

    metadata.primary_key = KeyDescription::getKeyFromAST(args.storage_def->primary_key->ptr(), metadata.columns, args.getContext());
    auto primary_key_names = metadata.getColumnsRequiredForPrimaryKey();
    if (primary_key_names.size() != 1)
    {
        throw Exception("StorageEmbeddedRocksDB must require one column in primary key", ErrorCodes::BAD_ARGUMENTS);
    }
    return StorageEmbeddedRocksDB::create(
        args.table_id, args.relative_data_path, metadata, args.attach, args.getContext(), primary_key_names[0]);
}

StorageEmbeddedRocksDB::StorageEmbeddedRocksDB(
    const StorageID & table_id_,
    const String & relative_data_path_,
    const StorageInMemoryMetadata & metadata_,
    bool attach,
    ContextPtr context_,
    const String & primary_key_)
    : IStorageKV(table_id_, primary_key_), WithContext(context_->getGlobalContext())
{
    setInMemoryMetadata(metadata_);
    rocksdb_dir = context_->getPath() + relative_data_path_;
    if (!attach)
    {
        fs::create_directories(rocksdb_dir);
    }
    initDB();
}

void StorageEmbeddedRocksDB::truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &)
{
    std::unique_lock<std::shared_mutex> lock(rocksdb_ptr_mx);
    rocksdb_ptr->Close();
    rocksdb_ptr = nullptr;

    fs::remove_all(rocksdb_dir);
    fs::create_directories(rocksdb_dir);
    initDB();
}

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

void StorageEmbeddedRocksDB::initDB()
{
    rocksdb::Status status;
    rocksdb::Options base;
    rocksdb::DB * db;

    base.create_if_missing = true;
    base.compression = rocksdb::CompressionType::kZSTD;
    base.statistics = rocksdb::CreateDBStatistics();
    /// It is too verbose by default, and in fact we don't care about rocksdb logs at all.
    base.info_log_level = rocksdb::ERROR_LEVEL;

    rocksdb::Options merged = base;

    const auto & config = getContext()->getConfigRef();
    if (config.has("rocksdb.options"))
    {
        auto config_options = getOptionsFromConfig(config, "rocksdb.options");
        status = rocksdb::GetDBOptionsFromMap(merged, config_options, &merged);
        if (!status.ok())
        {
            throw Exception(
                ErrorCodes::ROCKSDB_ERROR,
                "Fail to merge rocksdb options from 'rocksdb.options' at: {}: {}",
                rocksdb_dir,
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
                rocksdb_dir,
                status.ToString());
        }
    }

    if (config.has("rocksdb.tables"))
    {
        auto table_name = getStorageID().getTableName();

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
                        rocksdb_dir,
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
                        rocksdb_dir,
                        status.ToString());
                }
            }
        }
    }

    status = rocksdb::DB::Open(merged, rocksdb_dir, &db);

    if (!status.ok())
    {
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "Fail to open rocksdb path at: {}: {}", rocksdb_dir, status.ToString());
    }
    rocksdb_ptr = std::unique_ptr<rocksdb::DB>(db);
}

std::shared_ptr<rocksdb::Statistics> StorageEmbeddedRocksDB::getRocksDBStatistics() const
{
    std::shared_lock<std::shared_mutex> lock(rocksdb_ptr_mx);
    if (!rocksdb_ptr)
        return nullptr;
    return rocksdb_ptr->GetOptions().statistics;
}

StorageKVReaderPtr StorageEmbeddedRocksDB::getReader()
{
    return std::make_shared<StorageEmbeddedRocksDBReader>(*this);
}

StorageKVWriterPtr StorageEmbeddedRocksDB::getWriter()
{
    return std::make_shared<StorageEmbeddedRocksDBWriter>(*this);
}

void registerStorageEmbeddedRocksDB(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_sort_order = true,
        .supports_parallel_insert = true,
    };

    factory.registerStorage("EmbeddedRocksDB", create, features);
}

};
