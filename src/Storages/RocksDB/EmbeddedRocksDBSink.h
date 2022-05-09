#pragma once

#include <Processors/Sinks/SinkToStorage.h>
#include <Common/logger_useful.h>

namespace DB
{

class StorageEmbeddedRocksDB;
struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

class EmbeddedRocksDBSink : public SinkToStorage
{
public:
    EmbeddedRocksDBSink(
        StorageEmbeddedRocksDB & storage_,
        const StorageMetadataPtr & metadata_snapshot_);

    void consume(Chunk chunk) override;
    String getName() const override { return "EmbeddedRocksDBSink"; }

private:
    StorageEmbeddedRocksDB & storage;
    StorageMetadataPtr metadata_snapshot;
    size_t primary_key_pos = 0;

    Poco::Logger * log = &Poco::Logger::get("EmbeddedRocksDBSink");
};

}
