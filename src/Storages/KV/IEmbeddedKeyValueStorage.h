#pragma once

#include <memory>
#include <string>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>

namespace DB
{

using String = std::string;
class Context;

struct EmbeddedKeyValueStorageOptions
{
    String table_name;
    String rocksdb_dir;
    const Poco::Util::AbstractConfiguration & config;
    Int32 ttl;
    bool read_only;
};

class IEmbeddedKeyValueStorage
{
public:
    virtual ~IEmbeddedKeyValueStorage() = default;

    class Status
    {
    public:
        enum Code : unsigned char
        {
            kOk = 0,
            kNotFound = 1,
            kCorruption = 2,
            kNotSupported = 3,
            kInvalidArgument = 4,
            kIOError = 5,
            kMergeInProgress = 6,
            kIncomplete = 7,
            kShutdownInProgress = 8,
            kTimedOut = 9,
            kAborted = 10,
            kBusy = 11,
            kExpired = 12,
            kTryAgain = 13,
            kCompactionTooLarge = 14,
            kColumnFamilyDropped = 15,
            kMaxCode
        };

    public:
        Status(Code code_) : code(code_) { }

        bool ok() { return code == Code::kOk; }

        String toString() const { return "Test"; }

    private:
        const Code code;
    };

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

        virtual Status put(String & key, String & value);
        virtual Status remove(String & key);
        virtual Status commit();
    };

    virtual void initDB(EmbeddedKeyValueStorageOptions & options) = 0;
    virtual void truncate(EmbeddedKeyValueStorageOptions & options) = 0;

    using Reader = std::unique_ptr<IEmbeddedKeyValueStorage::ReadIterator>;
    using Writer = std::unique_ptr<IEmbeddedKeyValueStorage::WriteIterator>;

    virtual Reader getReader() const = 0;
    virtual Writer getWriter() = 0;
};

using EmbeddedKeyValueStoragePtr = std::shared_ptr<IEmbeddedKeyValueStorage>;

};
