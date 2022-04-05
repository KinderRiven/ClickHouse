#pragma once

#include <queue>
#include <Core/BackgroundSchedulePool.h>
#include <base/logger_useful.h>
#include <pcg_random.hpp>
#include <Common/ThreadPool.h>

namespace DB
{

class SliceReadBuffer;
class SliceManagement;
struct SliceDownloadMetadata;

enum CacheTaskType
{
    CACHE_CLEANUP,
    CACHE_PREFETCH,
};

/// Settings for background tasks scheduling. Each background assignee has one
/// BackgroundSchedulingPoolTask and depending on execution result may put this
/// task to sleep according to settings. Look at scheduleTask function for details.
struct CacheTaskSchedulingSettings
{
    double thread_sleep_seconds_random_part = 1.0;
    double thread_sleep_seconds_if_nothing_to_do = 0.1;
    double task_sleep_seconds_when_no_work_max = 600;
    /// For exponential backoff.
    double task_sleep_seconds_when_no_work_multiplier = 1.1;

    double task_sleep_seconds_when_no_work_random_part = 1.0;

    /// Deprecated settings, don't affect background execution
    double thread_sleep_seconds = 10;
    double task_sleep_seconds_when_no_work_min = 10;
};

class CacheJobsAssignee : public WithContext
{
private:
    CacheTaskType type;

    /// Settings for execution control of background scheduling task
    CacheTaskSchedulingSettings sleep_settings;
    /// Useful for random backoff timeouts generation
    pcg64 rng;
    /// How many times execution of background job failed or we have
    /// no new jobs.
    std::atomic<size_t> no_work_done_count{0};

    /// Scheduling task which assign jobs in background pool
    BackgroundSchedulePool::TaskHolder holder;
    /// Mutex for thread safety
    std::mutex holder_mutex;

    Poco::Logger * trace_log = &Poco::Logger::get("[CacheJobsAssignee]");

public:
    /// In case of ReplicatedMergeTree the first assignee will be responsible for
    /// polling the replication queue and schedule operations according to the LogEntry type
    /// e.g. merges, mutations and fetches. The same will be for Plain MergeTree except there is no
    /// replication queue, so we will just scan parts and decide what to do.
    /// Moving operations are the same for all types of MergeTree and also have their own timetable.
    void start();
    void trigger();
    void postpone();
    void finish();

    /// Just call finish
    ~CacheJobsAssignee();

    CacheJobsAssignee(CacheTaskType type, ContextPtr global_context_);

private:
    /// Function that executes in background scheduling pool
    void threadFunc();
};


}
