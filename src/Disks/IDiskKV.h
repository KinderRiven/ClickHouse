#pragma once

#include <Disks/DiskType.h>
#include <Disks/IDisk.h>
#include <Common/config.h>

#include <atomic>
#include <filesystem>
#include <utility>
#include <Disks/DiskFactory.h>
#include <Disks/Executor.h>
#include <Common/MultiVersion.h>
#include <Common/ThreadPool.h>

namespace DB
{
class IDiskKV : public IDisk
{
public:
    /// The disk under the KV model does not have a path.
    const String & getPath() const final override { return ""; }

    UInt64 getTotalSpace() const override { return std::numeric_limits<UInt64>::max(); }

    UInt64 getAvailableSpace() const override { return std::numeric_limits<UInt64>::max(); }

    UInt64 getUnreservedSpace() const override { return std::numeric_limits<UInt64>::max(); }

    UInt64 getKeepingFreeSpace() const override { return 0; }

    /// We treat a key-value pair as a file, check if it exists.
    bool isFile(const String &) const override { return true; }

    /// Disk does not exist in the directory under the KV model.
    /// This function always returns false.
    bool isDirectory(const String &) const override { return false; }

    /// empty function, throw error.
    void createDirectory(const String &) override { }

    /// empty function, throw error
    void createDirectories(const String &) override { }

    /// empty function, throw error
    void clearDirectory(const String &) override { }

    /// empty function, throw error
    void moveDirectory(const String &, const String &) override { }

    DiskDirectoryIteratorPtr iterateDirectory(const String &) override { return nullptr; }

    /// empty function, throw error.
    void moveFile(const String &, const String &) override { }

    /// empty function, throw error.
    void replaceFile(const String &, const String &) override { }

    /// empty function, throw error.
    void copy(const String &, const std::shared_ptr<IDisk> &, const String &) override { }

    /// empty function, throw error.
    void listFiles(const String &, std::vector<String> &) override { }

    /// empty function, throw error.
    void removeDirectory(const String &) override { }

    /// empty function, throw error.
    void removeRecursive(const String &) override { }

    void removeSharedFile(const String & path, bool) override { removeFile(path); }

    void removeSharedRecursive(const String & path, bool) override { removeRecursive(path); }

    void removeSharedFileIfExists(const String & path, bool) override { removeFileIfExists(path); }

    /// unknow function
    void setLastModified(const String &, const Poco::Timestamp &) override { }

    /// unknow function
    Poco::Timestamp getLastModified(const String &) override { return Poco::Timestamp{}; }

    void setReadOnly(const String &) override { }

    void createHardLink(const String &, const String &) override { }

    void truncateFile(const String &, size_t) override { }

    bool isRemote() const override { return true; }

    bool supportZeroCopyReplication() const override { return false; }

    bool isReadOnly() const override { return false; }

    ReservationPtr reserve(UInt64) override { return nullptr; }

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
};
};