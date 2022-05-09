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
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/IStorageKV.h>
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
}

class KVSource : public SourceWithProgress
{
public:
    KVSource(
        const IStorageKV & storage_,
        StorageKVReaderPtr reader_,
        const Block & header,
        FieldVectorPtr keys_,
        FieldVector::const_iterator begin_,
        FieldVector::const_iterator end_,
        const size_t max_block_size_)
        : SourceWithProgress(header)
        , storage(storage_)
        , reader(reader_)
        , primary_key_pos(header.getPositionByName(storage.getPrimaryKey()))
        , keys(keys_)
        , begin(begin_)
        , end(end_)
        , it(begin)
        , max_block_size(max_block_size_)
    {
    }

    /// all scan
    KVSource(const IStorageKV & storage_, const StorageKVReaderPtr & reader_, const Block & header, const size_t max_block_size_)
        : SourceWithProgress(header)
        , storage(storage_)
        , reader(reader_)
        , primary_key_pos(header.getPositionByName(storage.getPrimaryKey()))
        , max_block_size(max_block_size_)
    {
    }

    String getName() const override { return storage.getName(); }

    Chunk generate() override
    {
        if (keys)
            return generateWithKeys();
        return generateFullScan();
    }

    Chunk generateWithKeys()
    {
        if (it >= end)
            return {};

        size_t num_keys = end - begin;

        std::vector<String> serialized_keys(num_keys);
        std::vector<String> values;

        const auto & sample_block = getPort().getHeader();
        const auto & key_column_type = sample_block.getByName(storage.getPrimaryKey()).type;

        size_t rows_processed = 0;
        while (it < end && rows_processed < max_block_size)
        {
            WriteBufferFromString wb(serialized_keys[rows_processed]);
            key_column_type->getDefaultSerialization()->serializeBinary(*it, wb);
            wb.finalize();
            ++it;
            ++rows_processed;
        }

        MutableColumns columns = sample_block.cloneEmptyColumns();
        auto statuses = reader->readRowsWithKeys(serialized_keys, values);

        for (size_t i = 0; i < statuses.size(); ++i)
        {
            if (statuses[i] == IStorageKVReader::Status::FOUND)
            {
                ReadBufferFromString key_buffer(serialized_keys[i]);
                ReadBufferFromString value_buffer(values[i]);
                fillColumns(key_buffer, value_buffer, columns);
            }
        }
        UInt64 num_rows = columns.at(0)->size();
        return Chunk(std::move(columns), num_rows);
    }

    Chunk generateFullScan()
    {
        if (!reader->Valid())
            return {};

        const auto & sample_block = getPort().getHeader();
        MutableColumns columns = sample_block.cloneEmptyColumns();

        for (size_t rows = 0; reader->Valid() && rows < max_block_size; ++rows)
        {
            String key, value;
            reader->readRow(key, value);

            ReadBufferFromString key_buffer(key);
            ReadBufferFromString value_buffer(value);

            fillColumns(key_buffer, value_buffer, columns);
        }
        Block block = sample_block.cloneWithColumns(std::move(columns));
        return Chunk(block.getColumns(), block.rows());
    }

    void fillColumns(ReadBufferFromString & key_buffer, ReadBufferFromString & value_buffer, MutableColumns & columns)
    {
        size_t idx = 0;
        for (const auto & elem : getPort().getHeader())
        {
            elem.type->getDefaultSerialization()->deserializeBinary(*columns[idx], idx == primary_key_pos ? key_buffer : value_buffer);
            ++idx;
        }
    }

private:
    const IStorageKV & storage;
    StorageKVReaderPtr reader;
    size_t primary_key_pos;

    /// For key scan
    FieldVectorPtr keys = nullptr;
    FieldVector::const_iterator begin;
    FieldVector::const_iterator end;
    FieldVector::const_iterator it;

    const size_t max_block_size;
};

class KVSink : public SinkToStorage
{
public:
    KVSink(IStorageKV & storage_, StorageKVWriterPtr writer_, const StorageMetadataPtr & metadata_snapshot_)
        : SinkToStorage(metadata_snapshot_->getSampleBlock()), storage(storage_), writer(writer_), metadata_snapshot(metadata_snapshot_)
    {
        for (const auto & elem : getHeader())
        {
            if (elem.name == storage.primary_key)
                break;
            ++primary_key_pos;
        }
    }

    void consume(Chunk chunk) override
    {
        auto rows = chunk.getNumRows();
        auto block = getHeader().cloneWithColumns(chunk.detachColumns());

        WriteBufferFromOwnString wb_key;
        WriteBufferFromOwnString wb_value;

        for (size_t i = 0; i < rows; ++i)
        {
            wb_key.restart();
            wb_value.restart();
            size_t idx = 0;
            for (const auto & elem : block)
            {
                elem.type->getDefaultSerialization()->serializeBinary(*elem.column, i, idx == primary_key_pos ? wb_key : wb_value);
                ++idx;
            }
            writer->Add(wb_key.str(), wb_value.str());
        }
        writer->Commit();
    }

    String getName() const override { return storage.getName(); }

private:
    IStorageKV & storage;
    StorageKVWriterPtr writer;
    StorageMetadataPtr metadata_snapshot;
    size_t primary_key_pos = 0;
};

IStorageKVReader::~IStorageKVReader() = default;

IStorageKVWriter::~IStorageKVWriter() = default;

// returns keys may be filter by condition
static bool traverseASTFilter(
    const String & primary_key, const DataTypePtr & primary_key_type, const ASTPtr & elem, const PreparedSets & sets, FieldVectorPtr & res)
{
    const auto * function = elem->as<ASTFunction>();
    if (!function)
        return false;

    if (function->name == "and")
    {
        // one child has the key filter condition is ok
        for (const auto & child : function->arguments->children)
            if (traverseASTFilter(primary_key, primary_key_type, child, sets, res))
                return true;
        return false;
    }
    else if (function->name == "or")
    {
        // make sure every child has the key filter condition
        for (const auto & child : function->arguments->children)
            if (!traverseASTFilter(primary_key, primary_key_type, child, sets, res))
                return false;
        return true;
    }
    else if (function->name == "equals" || function->name == "in")
    {
        const auto & args = function->arguments->as<ASTExpressionList &>();
        const ASTIdentifier * ident;
        const IAST * value;

        if (args.children.size() != 2)
            return false;

        if (function->name == "in")
        {
            ident = args.children.at(0)->as<ASTIdentifier>();
            if (!ident)
                return false;

            if (ident->name() != primary_key)
                return false;
            value = args.children.at(1).get();

            PreparedSetKey set_key;
            if ((value->as<ASTSubquery>() || value->as<ASTIdentifier>()))
                set_key = PreparedSetKey::forSubquery(*value);
            else
                set_key = PreparedSetKey::forLiteral(*value, {primary_key_type});

            auto set_it = sets.find(set_key);
            if (set_it == sets.end())
                return false;
            SetPtr prepared_set = set_it->second;

            if (!prepared_set->hasExplicitSetElements())
                return false;

            prepared_set->checkColumnsNumber(1);
            const auto & set_column = *prepared_set->getSetElements()[0];
            for (size_t row = 0; row < set_column.size(); ++row)
                res->push_back(set_column[row]);
            return true;
        }
        else
        {
            if ((ident = args.children.at(0)->as<ASTIdentifier>()))
                value = args.children.at(1).get();
            else if ((ident = args.children.at(1)->as<ASTIdentifier>()))
                value = args.children.at(0).get();
            else
                return false;

            if (ident->name() != primary_key)
                return false;

            /// function->name == "equals"
            if (const auto * literal = value->as<ASTLiteral>())
            {
                auto converted_field = convertFieldToType(literal->value, *primary_key_type);
                if (!converted_field.isNull())
                    res->push_back(converted_field);
                return true;
            }
        }
    }
    return false;
}

/** Retrieve from the query a condition of the form `key = 'key'`, `key in ('xxx_'), from conjunctions in the WHERE clause.
  * TODO support key like search
  */
static std::pair<FieldVectorPtr, bool>
getFilterKeys(const String & primary_key, const DataTypePtr & primary_key_type, const SelectQueryInfo & query_info)
{
    const auto & select = query_info.query->as<ASTSelectQuery &>();
    if (!select.where())
        return {{}, true};

    FieldVectorPtr res = std::make_shared<FieldVector>();
    auto matched_keys = traverseASTFilter(primary_key, primary_key_type, select.where(), query_info.sets, res);
    return std::make_pair(res, !matched_keys);
}

IStorageKV::IStorageKV(const StorageID & table_id_, const String & primary_key_) : IStorage(table_id_), primary_key(primary_key_)
{
}

Pipe IStorageKV::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned num_streams)
{
    storage_snapshot->check(column_names);

    FieldVectorPtr keys;
    bool all_scan = false;

    Block sample_block = storage_snapshot->metadata->getSampleBlock();
    auto primary_key_data_type = sample_block.getByName(primary_key).type;
    std::tie(keys, all_scan) = getFilterKeys(primary_key, primary_key_data_type, query_info);
    if (all_scan)
    {
        auto reader = getReader();
        return Pipe(std::make_shared<KVSource>(*this, reader, sample_block, max_block_size));
    }
    else
    {
        if (keys->empty())
            return {};

        ::sort(keys->begin(), keys->end());
        keys->erase(std::unique(keys->begin(), keys->end()), keys->end());

        Pipes pipes;

        size_t num_keys = keys->size();
        size_t num_threads = std::min<size_t>(num_streams, keys->size());

        assert(num_keys <= std::numeric_limits<uint32_t>::max());
        assert(num_threads <= std::numeric_limits<uint32_t>::max());

        for (size_t thread_idx = 0; thread_idx < num_threads; ++thread_idx)
        {
            size_t begin = num_keys * thread_idx / num_threads;
            size_t end = num_keys * (thread_idx + 1) / num_threads;
            auto reader = getReader();
            pipes.emplace_back(
                std::make_shared<KVSource>(*this, reader, sample_block, keys, keys->begin() + begin, keys->begin() + end, max_block_size));
        }
        return Pipe::unitePipes(std::move(pipes));
    }
}

SinkToStoragePtr IStorageKV::write(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr)
{
    auto writer = getWriter();
    return std::make_shared<KVSink>(*this, writer, metadata_snapshot);
}

};
