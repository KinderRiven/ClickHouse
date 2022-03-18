#include <IO/WriteBufferFromKVBase.h>

namespace DB
{

WriteBufferFromKVBase::WriteBufferFromKVBase(size_t value_length_)
    : BufferWithOwnMemory<WriteBuffer>(value_length_, nullptr, 0), value_length(value_length_)
{
}

WriteBufferFromKVBase::~WriteBufferFromKVBase() = default;

};