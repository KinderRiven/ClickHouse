#pragma once

#include <mutex>
#include <vector>
#include <Storages/IStorageKV.h>
#include <rocksdb/db.h>
#include <rocksdb/status.h>
#include <Common/logger_useful.h>


namespace DB
{

class StorageEmbeddedRocksDB;
using RocksDBPtr = std::unique_ptr<rocksdb::DB>;

class StorageEmbeddedRocksDBReader final : public IStorageKVReader
{
public:
    StorageEmbeddedRocksDBReader(StorageEmbeddedRocksDB & rocksdb_);

    ~StorageEmbeddedRocksDBReader() override;

    Status readRow(String & key, String & value) const override;

    std::vector<Status> readRows(std::vector<String> & keys, std::vector<String> & values, size_t max_rows) const override;

    Status readRowWithKey(const String & key, String & value) const override;

    std::vector<Status> readRowsWithKeys(const std::vector<String> & keys, std::vector<String> & values) const override;

public:
    bool Valid() const override;

    void SeekToFirst() override;

    void Seek(String & key) override;

    void Next() override;

    String key() const override;

    String value() const override;

private:
    StorageEmbeddedRocksDB & rocksdb;
    mutable std::unique_ptr<rocksdb::Iterator> iterator = nullptr;
};

class StorageEmbeddedRocksDBWriter final : public IStorageKVWriter
{
public:
    StorageEmbeddedRocksDBWriter(StorageEmbeddedRocksDB & rocksdb_);

    ~StorageEmbeddedRocksDBWriter() override;

    Status Put(const String & key, const String & value) override;

    Status BatchPut(const std::vector<String> & keys, const std::vector<String> & values) override;

    Status Add(const String & key, const String & value) override;

    Status Commit() override;

private:
    StorageEmbeddedRocksDB & rocksdb;
};

class StorageEmbeddedRocksDB final : public shared_ptr_helper<StorageEmbeddedRocksDB>, public IStorageKV, WithContext
{
public:
    friend struct shared_ptr_helper<StorageEmbeddedRocksDB>;
    friend class StorageEmbeddedRocksDBWriter;
    friend class StorageEmbeddedRocksDBReader;

public:
    ~StorageEmbeddedRocksDB() override = default;

    std::string getName() const override { return "StorageEmbeddedRocksDB"; }

    StorageKVReaderPtr getReader() override;

    StorageKVWriterPtr getWriter() override;

    bool supportsParallelInsert() const override { return true; }

    bool supportsIndexForIn() const override { return true; }

    bool mayBenefitFromIndexForIn(
        const ASTPtr & node, ContextPtr /*query_context*/, const StorageMetadataPtr & /*metadata_snapshot*/) const override
    {
        return node->getColumnName() == primary_key;
    }

    bool storesDataOnDisk() const override { return true; }

    Strings getDataPaths() const override { return {rocksdb_dir}; }

    std::shared_ptr<rocksdb::Statistics> getRocksDBStatistics() const;

    void truncate(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr, TableExclusiveLockHolder &) override;

protected:
    StorageEmbeddedRocksDB(
        const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata,
        bool attach,
        ContextPtr context_,
        const String & primary_key_);

private:
    Poco::Logger * log = &Poco::Logger::get("StorageEmbeddedRocksDB");
    RocksDBPtr rocksdb_ptr;
    mutable std::shared_mutex rocksdb_ptr_mx;
    String rocksdb_dir;

    void initDB();
};

};
