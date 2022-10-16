#pragma once

#include <string>
#include <Storages/IStorage.h>
#include <Interpreters/Context.h>

namespace DB
{

using String = std::string;
class Context;

struct EmbeddedKeyValueStorageOptions
{
    StorageID storage_id;
    String rocksdb_dir;
    ContextPtr context;
    Int32 ttl;
    bool read_only;
};

class IEmbeddedKeyValueStorage
{
public:
    virtual ~IEmbeddedKeyValueStorage() = default;

    class ReadIterator
    {
    public:
        virtual ~ReadIterator() = default;

        virtual String key();
        virtual String value();

        virtual void seekToFirst();

        virtual void next();
        virtual bool valid();
    };

    class WriteIterator
    {
    public:
        virtual ~WriteIterator() = default;

        virtual bool put(String & key, String & value);
        virtual bool commit();
    };

    virtual void initDB(EmbeddedKeyValueStorageOptions & options) = 0;

    using Reader = std::unique_ptr<IEmbeddedKeyValueStorage::ReadIterator>;
    using Writer = std::unique_ptr<IEmbeddedKeyValueStorage::WriteIterator>;

    virtual Reader getReader() const = 0;
    virtual Writer getWriter() = 0;
};

};
