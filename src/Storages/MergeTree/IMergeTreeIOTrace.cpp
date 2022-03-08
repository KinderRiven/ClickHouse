#include "IMergeTreeIOTrace.h"

IMergeTreeIOTrace & IMergeTreeIOTrace::instance()
{
    static IMergeTreeIOTrace ret;
    return ret;
}