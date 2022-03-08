#pragma once

#include <base/logger_useful.h>

namespace DB {
class IMergeTreeIOTrace {
public:
    static IMergeTreeIOTrace & instance();

private:
    Poco::Logger * trace_log = &Poco::Logger::get("[MergeTreeIOTrace]");
};
};