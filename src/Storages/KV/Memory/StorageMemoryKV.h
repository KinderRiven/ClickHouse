#pragma once

#include <atomic>
#include <mutex>
#include <unordered_map>
#include <Storages/IStorageKV.h>
#include <Common/logger_useful.h>

namespace DB
{

class Context;
class MemoryKV;
class StorageMemoryKV;
using MemoryKVPtr = std::shared_ptr<MemoryKV>;

class MemoryKV
{
public:
    using SortMap = std::map<String, String>;
    using SortMapPtr = std::shared_ptr<SortMap>;
    using SortMapIterator = SortMap::iterator;

    class Iterator
    {
    public:
        Iterator(SortMapPtr map_) : map(std::move(map_)), iterator(map->begin()) { }

        void SeekToFirst() { iterator = map->begin(); }

        void Seek(String & key) { iterator = map->find(key); }

        bool Valid() const { return (iterator != map->end()) ? true : false; }

        void Next()
        {
            if (iterator != map->end())
                iterator++;
        }

        String key() const { return iterator->first; }

        String value() const { return iterator->second; }

    private:
        SortMapPtr map;
        SortMapIterator iterator;
    };

public:
    MemoryKV() : map(std::make_shared<SortMap>()) { }

    Iterator getNewIterator()
    {
        Iterator iter(map);
        return iter;
    }

    bool Put(const String & key, const String & value)
    {
        std::lock_guard kv_lock(lock);
        map->insert({key, value});
        return true;
    }

    bool Get(const String & key, String & value)
    {
        std::lock_guard kv_lock(lock);
        auto iter = map->find(key);
        if (iter != map->end())
        {
            value = iter->second;
            return true;
        }
        return false;
    }

private:
    SortMapPtr map;
    std::mutex lock;
};

class StorageMemoryKVReader final : public IStorageKVReader
{
public:
    StorageMemoryKVReader(StorageMemoryKV & storage_);

    ~StorageMemoryKVReader() override;

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
    StorageMemoryKV & storage;
    mutable MemoryKV::Iterator iterator;
};

class StorageMemoryKVWriter final : public IStorageKVWriter
{
public:
    StorageMemoryKVWriter(StorageMemoryKV & storage_);

    ~StorageMemoryKVWriter() override;

    Status Put(const String & key, const String & value) override;

    Status BatchPut(const std::vector<String> & keys, const std::vector<String> & values) override;

    Status Add(const String & key, const String & value) override;

    Status Commit() override;

private:
    StorageMemoryKV & storage;
};

class StorageMemoryKV final : public shared_ptr_helper<StorageMemoryKV>, public IStorageKV, WithContext
{
public:
    friend struct shared_ptr_helper<StorageMemoryKV>;
    friend class StorageMemoryKVWriter;
    friend class StorageMemoryKVReader;

public:
    ~StorageMemoryKV() override = default;

    std::string getName() const override { return "StorageMemoryKV"; }

    void truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &) override { }

    StorageKVReaderPtr getReader() override;

    StorageKVWriterPtr getWriter() override;

protected:
    StorageMemoryKV(
        const StorageID & table_id_, const StorageInMemoryMetadata & metadata, ContextPtr context_, const String & primary_key_);

private:
    MemoryKVPtr kv;
};

};
