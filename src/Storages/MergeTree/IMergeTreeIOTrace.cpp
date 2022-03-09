#include "IMergeTreeIOTrace.h"

using namespace DB;

IMergeTreeIOTrace & IMergeTreeIOTrace::instance()
{
    static IMergeTreeIOTrace ret;
    return ret;
}