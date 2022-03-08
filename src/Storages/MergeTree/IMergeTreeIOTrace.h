#pragma once

#include <base/logger_useful.h>

using namespace DB {
class IMergeTreeIOTrace {
public:
    static IMergeTreeIOTrace & instance();

private:
    Poco::Logger * trace_log = &Poco::Logger::get("[MergeTreeIOTrace]");
};
};