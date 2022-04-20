#include "IDiskKV.h"

namespace DB
{

IDiskKV::IDiskKV(const String & name_, const String & log_name_) : log(&Poco::Logger::get(log_name_)), name(name_)
{
}

};