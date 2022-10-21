#include <catch2/catch.hpp>
#include <messages/iv_msg.pb.h>
#include <sw/redis++/redis++.h>

TEST_CASE("Persist Protobuf To Redis")
{
    SECTION("Write To Redis Testing")
    {

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

        std::string msg {};
        auto result = record.SerializeToString(&msg);
        REQUIRE(result==true);

        sw::redis::Redis client( "tcp://localhost:6379" );
        result = client.set("TEST-KEY", msg);

        REQUIRE(result==true);
    }

    SECTION("Read From Redis Testing")
    {
        sw::redis::Redis client( "tcp://localhost:6379" );
        auto result = client.get("TEST-KEY");
        REQUIRE(result.has_value() == true);

        BFCHI::iv_record record;
        auto success = record.ParseFromString(result.value());
        REQUIRE(success);

        REQUIRE(record.props().root()=="AAPL");
        client.del("TEST-KEY");
    }
}