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
#include <Storages/KV/Memory/StorageMemoryKV.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageFactory.h>
#include <base/sort.h>
#include <Poco/Logger.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNSUPPORTED_METHOD;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

StorageMemoryKVReader::StorageMemoryKVReader(StorageMemoryKV & storage_) : storage(storage_), iterator(storage_.kv->getNewIterator())
{
}

StorageMemoryKVReader::~StorageMemoryKVReader() = default;

StorageMemoryKVReader::Status StorageMemoryKVReader::readRow(String & key, String & value) const
{
    if (!iterator.Valid())
        return StorageMemoryKVReader::Status::ERROR;

    key = iterator.key();
    value = iterator.value();
    iterator.Next();
    return StorageMemoryKVReader::Status::FOUND;
}

std::vector<StorageMemoryKVReader::Status> StorageMemoryKVReader::readRows(std::vector<String> &, std::vector<String> &, size_t) const
{
    throw Exception("StorageMemoryKV not support 'readRows'", ErrorCodes::UNSUPPORTED_METHOD);
}

StorageMemoryKVReader::Status StorageMemoryKVReader::readRowWithKey(const String &, String &) const
{
    throw Exception("StorageMemoryKV not support 'readRowWithKey'", ErrorCodes::UNSUPPORTED_METHOD);
}

std::vector<StorageMemoryKVReader::Status>
StorageMemoryKVReader::readRowsWithKeys(const std::vector<String> & keys, std::vector<String> & values) const
{
    String value;
    std::vector<StorageMemoryKVReader::Status> result;
    for (const auto & key : keys)
    {
        auto insert = storage.kv->Get(key, value);
        if (insert)
        {
            values.emplace_back(value);
            result.emplace_back(StorageMemoryKVReader::Status::FOUND);
        }
        else
            result.emplace_back(StorageMemoryKVReader::Status::NOT_FOUND);
    }
    return result;
}

bool StorageMemoryKVReader::Valid() const
{
    return iterator.Valid();
}

void StorageMemoryKVReader::SeekToFirst()
{
    iterator.SeekToFirst();
}

void StorageMemoryKVReader::Seek(String & key)
{
    iterator.Seek(key);
}

void StorageMemoryKVReader::Next()
{
    iterator.Next();
}

String StorageMemoryKVReader::key() const
{
    return iterator.key();
}

String StorageMemoryKVReader::value() const
{
    return iterator.value();
}

StorageMemoryKVWriter::StorageMemoryKVWriter(StorageMemoryKV & storage_) : storage(storage_)
{
}

StorageMemoryKVWriter::~StorageMemoryKVWriter() = default;

StorageMemoryKVWriter::Status StorageMemoryKVWriter::Put(const String & key, const String & value)
{
    return storage.kv->Put(key, value) ? StorageMemoryKVWriter::Status::OK : StorageMemoryKVWriter::Status::ERROR;
}

StorageMemoryKVWriter::Status StorageMemoryKVWriter::BatchPut(const std::vector<String> &, const std::vector<String> &)
{
    throw Exception("StorageMemoryKV not support 'BatchPut'", ErrorCodes::UNSUPPORTED_METHOD);
}

StorageMemoryKVWriter::Status StorageMemoryKVWriter::Add(const String & key, const String & value)
{
    return Put(key, value);
}

StorageMemoryKVWriter::Status StorageMemoryKVWriter::Commit()
{
    return StorageMemoryKVWriter::Status::OK;
}

static StoragePtr create(const StorageFactory::Arguments & args)
{
    if (!args.engine_args.empty())
        throw Exception(
            "Engine " + args.engine_name + " doesn't support any arguments (" + toString(args.engine_args.size()) + " given)",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    StorageInMemoryMetadata metadata;
    metadata.setColumns(args.columns);
    metadata.setConstraints(args.constraints);

    if (!args.storage_def->primary_key)
        throw Exception("StorageMemoryKV must require one column in primary key", ErrorCodes::BAD_ARGUMENTS);

    metadata.primary_key = KeyDescription::getKeyFromAST(args.storage_def->primary_key->ptr(), metadata.columns, args.getContext());

    auto primary_key_names = metadata.getColumnsRequiredForPrimaryKey();
    if (primary_key_names.size() != 1)
        throw Exception("StorageMemoryKV must require one column in primary key", ErrorCodes::BAD_ARGUMENTS);

    return StorageMemoryKV::create(args.table_id, metadata, args.getContext(), primary_key_names[0]);
}

StorageMemoryKV::StorageMemoryKV(
    const StorageID & table_id_, const StorageInMemoryMetadata & metadata_, ContextPtr context_, const String & primary_key_)
    : IStorageKV(table_id_, primary_key_), WithContext(context_->getGlobalContext()), kv(std::make_shared<MemoryKV>())
{
    setInMemoryMetadata(metadata_);
}

StorageKVReaderPtr StorageMemoryKV::getReader()
{
    return std::make_shared<StorageMemoryKVReader>(*this);
}

StorageKVWriterPtr StorageMemoryKV::getWriter()
{
    return std::make_shared<StorageMemoryKVWriter>(*this);
}

void registerStorageMemoryKV(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_sort_order = true,
        .supports_parallel_insert = true,
    };

    factory.registerStorage("StorageMemoryKV", create, features);
}

};
