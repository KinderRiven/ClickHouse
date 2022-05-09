#include <Storages/RocksDB/EmbeddedRocksDBSink.h>
#include <Storages/RocksDB/StorageEmbeddedRocksDB.h>
#include <IO/WriteBufferFromString.h>

#include <rocksdb/db.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ROCKSDB_ERROR;
}

EmbeddedRocksDBSink::EmbeddedRocksDBSink(
    StorageEmbeddedRocksDB & storage_,
    const StorageMetadataPtr & metadata_snapshot_)
    : SinkToStorage(metadata_snapshot_->getSampleBlock())
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
{
    for (const auto & elem : getHeader())
    {
        if (elem.name == storage.primary_key)
            break;
        ++primary_key_pos;
    }
}

void EmbeddedRocksDBSink::consume(Chunk chunk)
{
    auto rows = chunk.getNumRows();
    auto block = getHeader().cloneWithColumns(chunk.detachColumns());

    WriteBufferFromOwnString wb_key;
    WriteBufferFromOwnString wb_value;

    rocksdb::WriteBatch batch;
    rocksdb::Status status;
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
        String key = wb_key.str();
        String value = wb_value.str();
        LOG_INFO(log, "key :{}/{}, value : {}/{}", key, key.size(), value, value.size());
        for (size_t j = 0; j < key.size(); j++)
        {
            auto ch = static_cast<uint8_t>(key[j]);
            LOG_INFO(log, "key[{}]={}", j, ch);
        }
        for (size_t j = 0; j < value.size(); j++)
        {
            auto ch = static_cast<uint8_t>(value[j]);
            LOG_INFO(log, "value[{}]={}", j, ch);
        }
        status = batch.Put(wb_key.str(), wb_value.str());
        if (!status.ok())
            throw Exception("RocksDB write error: " + status.ToString(), ErrorCodes::ROCKSDB_ERROR);
    }

    status = storage.rocksdb_ptr->Write(rocksdb::WriteOptions(), &batch);
    if (!status.ok())
        throw Exception("RocksDB write error: " + status.ToString(), ErrorCodes::ROCKSDB_ERROR);
}

}
