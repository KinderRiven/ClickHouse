#include <IO/ReadBufferFromKVBase.h>

namespace DB
{

ReadBufferFromKVBase::ReadBufferFromKVBase() : BufferWithOwnMemory<ReadBuffer>(0, nullptr, 0)
{
}

ReadBufferFromKVBase::~ReadBufferFromKVBase() = default;

};