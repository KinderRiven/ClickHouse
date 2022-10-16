#pragma once

#include <memory>
#include <Storages/KV/IEmbeddedKeyValueStorage.h>
#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/status.h>

namespace DB
{

class EmbeddedRocksDBStorage : public IEmbeddedKeyValueStorage
{
    using RocksDBPtr = std::shared_ptr<rocksdb::DB>;

public:
    class ReadIterator : public IEmbeddedKeyValueStorage::ReadIterator
    {
    public:
        ReadIterator(std::unique_ptr<rocksdb::Iterator> iterator_) : iterator(std::move(iterator_)) { }

        String key() override;
        String value() override;

        void seekToFirst() override;

        void next() override;
        bool valid() override;

    private:
        std::unique_ptr<rocksdb::Iterator> iterator;
    };

    class WriteIterator : public IEmbeddedKeyValueStorage::WriteIterator
    {
    public:
        WriteIterator(RocksDBPtr rocksdb_ptr_) : rocksdb_ptr(rocksdb_ptr_) { }

        bool put(String & key, String & value) override;

        bool commit() override;

    private:
        rocksdb::WriteBatch batch;
        RocksDBPtr rocksdb_ptr;
    };

    EmbeddedRocksDBStorage();

    void initDB(EmbeddedKeyValueStorageOptions & options) override;

    std::unique_ptr<IEmbeddedKeyValueStorage::ReadIterator> getReader() const override;

    std::unique_ptr<IEmbeddedKeyValueStorage::WriteIterator> getWriter() override;

private:
    RocksDBPtr rocksdb_ptr;
};

};
