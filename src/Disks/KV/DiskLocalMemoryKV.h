#pragma once

#include <bitset>
#include <random>
#include <utility>
#include <Disks/IDiskKV.h>

#include <boost/algorithm/string.hpp>

#include <base/FnTraits.h>
#include <base/scope_guard_safe.h>
#include <base/unit.h>

#include <Common/FileCache.h>
#include <Common/FileCacheFactory.h>
#include <Common/checkStackSize.h>
#include <Common/createHardLink.h>
#include <Common/getRandomASCIIString.h>
#include <Common/quoteString.h>
#include <Common/thread_local_rng.h>

#include <IO/ReadBufferFromS3.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/SeekAvoidingReadBuffer.h>
#include <IO/WriteBufferFromS3.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/threadPoolCallbackRunner.h>

#include <Disks/IO/AsynchronousReadIndirectBufferFromRemoteFS.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/IO/ReadIndirectBufferFromRemoteFS.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <Disks/IO/WriteIndirectBufferFromRemoteFS.h>
#include <Disks/RemoteDisksCommon.h>

namespace DB
{

class DiskLocalMemoryKV : public IDiskKV
{
public:
    DiskLocalMemoryKV(String name_, ContextPtr context_, SettingsPtr)
};

};
