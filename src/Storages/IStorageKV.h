#pragma once

#include <Storages/IStorage.h>

#include <map>
#include <shared_mutex>
#include <base/shared_ptr_helper.h>
#include <Common/logger_useful.h>

#include <Core/NamesAndTypes.h>
#include <Disks/IDisk.h>
#include <Storages/IStorage.h>
#include <Common/FileChecker.h>
#include <Common/escapeForFileName.h>

namespace DB
{

///
/// Provides query interfaces for the embedded KV storage system. On the one hand,
/// it can read data in a way similar to iterators,  and on the other hand, it can
/// directly obtain data through interfaces such as Get(key).
///
/// Not all interfaces have to be implemented and used.
class IStorageKVReader
{
public:
    enum Status
    {
        FOUND,
        NOT_FOUND,
        ERROR,
    };

public:
    virtual ~IStorageKVReader() = 0;

    /// Read a KV pair from the current iterator position, then call iterator->next().
    virtual Status readRow(String & key, String & value) const = 0;

    /// Read multiple KV pairs from the current iterator position.
    /// In this process, iterator->next() will be used multiple times.
    virtual std::vector<Status> readRows(std::vector<String> & keys, std::vector<String> & values, size_t max_rows) const = 0;

    /// Using key to get value, call the Get(key) implementation of the corresponding key-value storage.
    virtual Status readRowWithKey(const String & key, String & value) const = 0;

    /// Using multiple keys to get values, for some key-value storage, batch get may achieve better performance.
    virtual std::vector<Status> readRowsWithKeys(const std::vector<String> & keys, std::vector<String> & values) const = 0;

public:
    /// Whether the current reader can continue to read data.
    virtual bool Valid() const = 0;

    /// Moving iterator to point first key-value pair.
    /// iterator->seek_to_first()
    virtual void SeekToFirst() = 0;

    /// Moving iterator point to the key-value pair greater than or equal to the key.
    /// iterator->seek(key)
    virtual void Seek(String & key) = 0;

    /// iterator->next()
    virtual void Next() = 0;

    /// iterator->key()
    virtual String key() const = 0;

    /// iterator->value()
    virtual String value() const = 0;
};

class IStorageKVWriter
{
public:
    enum Status
    {
        OK,
        ERROR,
    };

public:
    virtual ~IStorageKVWriter() = 0;

    /// Insert a KV pair.
    virtual Status Put(const String & key, const String & value) = 0;

    /// Batch insert KV pairs.
    virtual Status BatchPut(const std::vector<String> & keys, const std::vector<String> & values) = 0;

    /// Write KV pairs into the buffer through Add(), and then call Commit() to
    /// submit these KV pairs in a batch.
    virtual Status Add(const String & key, const String & value) = 0;
    virtual Status Commit() = 0;
};

class KVSink;
class KVSource;
class Context;

using FieldVectorPtr = std::shared_ptr<FieldVector>;
using StorageKVReaderPtr = std::shared_ptr<IStorageKVReader>;
using StorageKVWriterPtr = std::shared_ptr<IStorageKVWriter>;

class IStorageKV : public IStorage
{
public:
    friend class KVSink;
    friend class KVSource;

    IStorageKV(const StorageID & table_id_, const String & primary_key_);

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context) override;

    const String & getPrimaryKey() const { return primary_key; }

protected:
    virtual StorageKVReaderPtr getReader() = 0;

    virtual StorageKVWriterPtr getWriter() = 0;

    const String primary_key;
};

};
