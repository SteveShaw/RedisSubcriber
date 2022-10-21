#pragma once

#include "table_base.h"
#include <messages/iv_msg.pb.h>

namespace bluefin::chicago::applications
{
struct iv_table : public table_base
{
public:
    explicit iv_table(arrow::MemoryPool* pool, bool use_local_time);

    void add_row(const BFCHI::iv_record& msg);

    arrow::Status finish(const char* csv_file) override;
};
}