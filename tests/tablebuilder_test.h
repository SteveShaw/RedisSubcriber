#pragma once
#include <catch2/catch.hpp>

#include <string>

#include <fcntl.h>
#include <sys/mman.h>
#include <messages/iv_msg.pb.h>
// #include <TableBuilder.h>
#include <iv_table.h>


TEST_CASE("TableBuilder", "Initialization")
{
    SECTION("Constructor")
    {
        using namespace bluefin::chicago::applications;
        bool failed = false;

        try
        {
            // TableBuilder tb(true);
            arrow::MemoryPool *pool = nullptr;
            arrow::jemalloc_memory_pool( &pool );

            iv_table ivt(pool, false);
        }
        catch(const std::exception& e)
        {
            failed = true;
        }

        REQUIRE_FALSE(failed);
    }

    SECTION("Add")
    {
        // using namespace bluefin::ivrecorder;
        using namespace bluefin::chicago::applications;
        BFCHI::iv_record record;
        auto props = record.mutable_props();

        props->set_option_id("EqwAxucMy0i/vCkZGZm4gA");
        props->set_root("AAPL");
        props->set_expiration(1663383600000ll);
        props->set_strike(401.0);
        props->set_callput(BFCHI::CallPut::call);
        
        auto level = record.mutable_level();
        level->set_bidprice(9.42);
        level->set_askprice(9.59);
        level->set_bidsize(256);
        level->set_asksize(2);
        level->set_uprice(393.62);

        auto theos = record.mutable_theos();
        theos->set_askvol(0.2745900350);
        theos->set_bidvol(0.2483217355);
        theos->set_delta(-0.8551024096);
        theos->set_vega(0.0669178893);
        theos->set_model_fitted_vol(0.2714246853);

        bool failed = false;

        try
        {
            // TableBuilder builder(true);
            // builder.AddRow<BFCHI::CallPut, BFCHI::CallPut::call>(record);
            arrow::MemoryPool *pool = nullptr;
            arrow::jemalloc_memory_pool( &pool );
            
            iv_table ivt(pool, true);
            ivt.add_row(record);
            auto status = ivt.finish("test.csv");
            if(!status.ok())
            {
                throw std::runtime_error(status.message());
            }
        }
        catch(const std::exception& e)
        {
            failed = true;
        }

        REQUIRE_FALSE(failed);
    }
}