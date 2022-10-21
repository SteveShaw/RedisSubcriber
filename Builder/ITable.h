#pragma once

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include "IRow.h"

namespace bluefin::chicago::applications
{
    struct ITable
    {
        virtual void add_row(IRow*) = 0;
        virtual arrow::Status finish(const char* ) = 0;
        virtual const char* get_schema_fp() = 0;
    };
}