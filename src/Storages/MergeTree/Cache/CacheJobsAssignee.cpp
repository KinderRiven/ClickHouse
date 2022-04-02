#include <random>
#include <Storages/MergeTree/Cache/CacheJobsAssignee.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <pcg_random.hpp>
#include <Common/CurrentMetrics.h>
#include <Common/randomSeed.h>

namespace DB
{

CacheJobsAssignee::CacheJobsAssignee(ContextPtr global_context_) : WithContext(global_context_), rng(randomSeed())
{
}


void CacheJobsAssignee::trigger()
{
    std::lock_guard lock(holder_mutex);

    if (!holder)
        return;

    no_work_done_count = 0;
    /// We have background jobs, schedule task as soon as possible
    holder->schedule();
}


void CacheJobsAssignee::postpone()
{
    std::lock_guard lock(holder_mutex);

    if (!holder)
        return;

    auto no_work_done_times = no_work_done_count.fetch_add(1, std::memory_order_relaxed);
    double random_addition = std::uniform_real_distribution<double>(0, sleep_settings.task_sleep_seconds_when_no_work_random_part)(rng);

    size_t next_time_to_execute = 1000
        * (std::min(
               sleep_settings.task_sleep_seconds_when_no_work_max,
               sleep_settings.thread_sleep_seconds_if_nothing_to_do
                   * std::pow(sleep_settings.task_sleep_seconds_when_no_work_multiplier, no_work_done_times))
           + random_addition);

    holder->scheduleAfter(next_time_to_execute, false);
}


void CacheJobsAssignee::scheduleDownloadTask()
{
    bool res = true;
    res ? trigger() : postpone();
}


void CacheJobsAssignee::start()
{
    std::lock_guard lock(holder_mutex);
    if (!holder)
        holder = getContext()->getSchedulePool().createTask("CacheJobsAssignee:", [this] { threadFunc(); });

    holder->activateAndSchedule();
}


void CacheJobsAssignee::finish()
{
    /// No lock here, because scheduled tasks could call trigger method
    if (holder)
    {
        holder->deactivate();
    }
}


void CacheJobsAssignee::threadFunc()
{
    LOG_TRACE(trace_log, "This is a cache jobs assignee test.");
}


CacheJobsAssignee::~CacheJobsAssignee()
{
    try
    {
        finish();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
