#pragma once
#include <catch2/catch.hpp>
#include <Application.h>

TEST_CASE("Test get_filename()")
{
    SECTION("increment file suffix")
    {
        using namespace bluefin::chicago::applications;
        Utils::SpdLoggerInit( "test.log", Utils::levelvol_receiver );
        Application app("test-suffix", "test-queue");
        auto data_dir = Utils::get_homedir();
        data_dir += "/data/equity";
        app.LoadConfig(Utils::channel_receiver_equity, data_dir.c_str());
        auto expect_filename = data_dir;
        expect_filename.push_back('/');
        expect_filename += Utils::channel_receiver_equity;
        expect_filename += '_';
        REQUIRE( expect_filename + "100.csv" == app.get_filename() );
    }
}