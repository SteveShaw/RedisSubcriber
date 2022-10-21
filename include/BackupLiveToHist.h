#pragma once

#include "MYSQL_Connector.h"
#include <sw/redis++/redis++.h>

namespace bluefin::chicago::applications
{
template<typename DBCONFIG>
struct backup_live_to_history
{
    static constexpr std::int64_t SECS_PER_DAY = 24 * 3600;

    void run()
    {
        using namespace std::chrono_literals;
        mysql_connector connector( DBCONFIG::user, DBCONFIG::pwd, DBCONFIG::host, DBCONFIG::db, DBCONFIG::port );

        sw::redis::Redis redis_client( "tcp://localhost:6379" );

        while(true)
        {
            auto backup_onoff = redis_client.get(Utils::backup_notifier);
            if(backup_onoff.has_value() && (backup_onoff.value() == "1"))
            {
                break;
            }
            std::this_thread::sleep_for(2000ms);
        }

        while( true )
        {
            auto opt_stop = redis_client.get( Utils::stop_notifier );
            if( opt_stop.has_value() && ( opt_stop.value() == "1" ) )
            {
                spdlog::get(Utils::levelvol_db)->info("Stop, now: {}", std::chrono::system_clock::now());
                break;
            }


            auto sample_time = std::chrono::system_clock::now();
            auto epoch_secs = std::chrono::duration_cast<std::chrono::seconds>(sample_time.time_since_epoch()).count();
            auto cur_second = (epoch_secs % SECS_PER_DAY) % 60;

            if(cur_second < 15)
            {
                std::this_thread::sleep_for(std::chrono::seconds(15 - cur_second));
            }
            else if(cur_second > 55)
            {
                std::this_thread::sleep_for(std::chrono::seconds(60-cur_second+14));
            }            

            //do_backup
            auto t1 = std::chrono::system_clock::now();
            auto succ = connector.save_to_history();
            auto t2 = std::chrono::system_clock::now();

            if(succ)
            {
                spdlog::get(Utils::levelvol_db)->info("backup complete, use: {} msecs", std::chrono::duration_cast<std::chrono::milliseconds>(t2-t1).count());
            }
            else
            {
                spdlog::get(Utils::levelvol_db)->info("backup failed, now: {}", std::chrono::system_clock::now());
            }

            auto now = std::chrono::system_clock::now();
            auto msecs = std::chrono::duration_cast<std::chrono::milliseconds>(now - sample_time).count();
            auto wait_msecs = 60 * 1000 - msecs;

            if(wait_msecs > 0)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds{wait_msecs});
            }

        }
    }
};
}