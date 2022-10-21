#pragma once
#include <string>

namespace bluefin::chicago::applications
{
    struct IRow
    {
        virtual const char* get_string(const char* field) = 0;
        virtual double get_double(const char* ) = 0;
        virtual int32_t get_int32(const char* ) = 0;
        virtual int64_t get_int64(const char* ) = 0;
    };
}