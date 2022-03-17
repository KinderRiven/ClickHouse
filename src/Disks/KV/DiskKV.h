#pragma once

#include "Disks/IDisk.h"
#include "SimpleKV.h"

namespace DB
{
class DiskKV : public IDisk
{
public:
    DiskKV(); /// just for test

    const String & getPath() const final override { return ""; }

    UInt64 getTotalSpace() const override { return std::numeric_limits<UInt64>::max(); }

    UInt64 getAvailableSpace() const override { return std::numeric_limits<UInt64>::max(); }

    UInt64 getUnreservedSpace() const override { return std::numeric_limits<UInt64>::max(); }

    UInt64 getKeepingFreeSpace() const override { return 0; }

    /// Check if a key exists
    /// kv_impl->exists(path)
    bool exists(const String & path) const override;

    /// We treat a key-value pair as a file
    bool isFile(const String &) const override { return true; }

    /// Always return false
    bool isDirectory(const String &) const override { return false; }

    /// return kv_impl->get(path).size();
    size_t getFileSize(const String & path) const override;

    /// TODO
    /// empty function, throw error
    void createDirectory(const String &) override { }

    /// TODO
    /// empty function, throw error
    void createDirectories(const String &) override { }

    /// TODO
    /// empty function, throw error
    void clearDirectory(const String &) override { }

    /// TODO
    /// empty function, throw error
    void moveDirectory(const String &, const String &) override { }

    DiskDirectoryIteratorPtr iterateDirectory(const String &) override { return nullptr; }

    /// kv_impl->put(path, empty_value)
    void createFile(const String & path) override;

    /// TODO
    /// empty function, throw error
    void moveFile(const String &, const String &) override { }

    /// TODO
    /// empty function, throw error
    void replaceFile(const String &, const String &) override { }

    /// TODO
    void copy(const String &, const std::shared_ptr<IDisk> &, const String &) override { }

    /// TODO
    void listFiles(const String &, std::vector<String> &) override { }

    std::unique_ptr<ReadBufferFromFileBase>
    readFile(const String & path, const ReadSettings & settings = ReadSettings{}, std::optional<size_t> size = {}) const override;

    std::unique_ptr<WriteBufferFromFileBase>
    writeFile(const String & path, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE, WriteMode mode = WriteMode::Rewrite) override;

    void removeFile(const String & path) override;

    void removeFileIfExists(const String & path) override;

    void removeDirectory(const String &) override { }

    void removeRecursive(const String &) override { }

    void removeSharedFile(const String & path, bool) override { removeFile(path); }

    void removeSharedRecursive(const String & path, bool) override { removeRecursive(path); }

    void removeSharedFileIfExists(const String & path, bool) override { removeFileIfExists(path); }

    /// unknow function
    void setLastModified(const String &, const Poco::Timestamp &) override { }

    /// unknow function
    Poco::Timestamp getLastModified(const String &) override { }

    void setReadOnly(const String &) override { }

    void createHardLink(const String &, const String &) override { }

    void truncateFile(const String &, size_t) override { }

    DiskType getType() const override { }

    bool isRemote() const override { return true; }

    bool supportZeroCopyReplication() const override { return false }

    bool isReadOnly() const override { return false; }

    void shutdown() override;

    void startup() override;

    /// Return some uniq string for file, overrode for IDiskRemote
    /// Required for distinguish different copies of the same part on remote disk
    String getUniqueId(const String & path) const override { return path; }

    /// Check file exists and ClickHouse has an access to it
    /// Overrode in remote FS disks (s3/hdfs)
    /// Required for remote disk to ensure that replica has access to data written by other node
    bool checkUniqueId(const String & id) const override { return exists(id); }

    /// Invoked on partitions freeze query.
    void onFreeze(const String &) override { }

    /// Returns guard, that insures synchronization of directory metadata with storage device.
    /// SyncGuardPtr getDirectorySyncGuard(const String & path) const override;

    /// Applies new settings for disk in runtime.
    void applyNewSettings(const Poco::Util::AbstractConfiguration &, ContextPtr, const String &, const DisksMap &) override { }

private:
    SimpleKV * kv_impl = nullptr;
};
};