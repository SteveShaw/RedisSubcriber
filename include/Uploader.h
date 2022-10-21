#pragma once

#include <sw/redis++/redis++.h>
#include <thread>
#include "Utils.h"
#include <jsoncpp/json/json.h>
#include <iostream>
#include <memory>
#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/daily_file_sink.h>


namespace bluefin::chicago::applications
{
/**
 * @brief Database uploader
 *
 */
template<typename TDBConn, typename DBConfig>
class Uploader
{
    static constexpr const char* uuid = "70ccd386-36c1-42c5-b75b-06635616d407";

    sw::redis::Redis m_redis;

    std::unique_ptr<TDBConn> m_connector;
    Json::Value m_json_obj;
    Json::Reader m_json_reader;

    //key to the file list in redis
    //the files are csv files to be uploaded to mysql

public:

    /**
     * @brief constructor
     *
     */
    explicit Uploader()
        : m_redis( "tcp://localhost:6379" )
        , m_connector(std::make_unique<TDBConn>(DBConfig::user, DBConfig::pwd, DBConfig::host, DBConfig::db, DBConfig::port))
    {}

    void on_message( std::string& msg )
    {
        std::string last_file {};
        try
        {
            m_json_reader.parse( msg, m_json_obj );

            auto tm = m_json_obj["Epoch"].asInt64();
            auto csv_file = m_json_obj["File"].asCString();
            auto t1 = std::chrono::system_clock::now();
            if(last_file != csv_file)
            {
                last_file = csv_file;
                m_connector->upload_from( csv_file );
            }
            auto t2 = std::chrono::system_clock::now();
            auto used_msecs = std::chrono::duration_cast<std::chrono::milliseconds>(t2-t1).count();
            spdlog::get( Utils::levelvol_db )->info( " FILE: {}, Epoch : {}, Upload: {} msecs", csv_file, tm, used_msecs );

            //send signal to backup thread to start backup
            m_redis.set(Utils::backup_notifier, "1");
        }
        catch( const std::exception& ex )
        {
            spdlog::get( Utils::levelvol_db )->error( "Error: {}", ex.what() );
        }
    }

    //key to the file list in redis
    //the files are csv files to be uploaded to mysql
    void run_by_subscribe( const char* channel_name )
    {

        auto subs = m_redis.subscriber();

        subs.subscribe(channel_name);
        subs.on_message([this](std::string channel, std::string msg){
            this->on_message(msg);
        });

        while( true )
        {
            auto opt_stop = m_redis.get( Utils::stop_notifier );
            if( opt_stop.has_value() && ( opt_stop.value() == "1" ) )
            {
                spdlog::get(Utils::levelvol_db)->info("Stop, now: {}", std::chrono::system_clock::now());
                break;
            }

            try
            {
                subs.consume();
            }
            catch( const sw::redis::Error &err )
            {
                // std::cerr << "Error: " << err.what() << '\n';
                spdlog::get(Utils::levelvol_receiver)->error("Error: {}", err.what());
                return;
            }
        }
    }

    void run_by_list( const char* channel_name)
    {
        while(true)
        {
            auto opt_stop = m_redis.get( Utils::stop_notifier );
            if( opt_stop.has_value() && ( opt_stop.value() == "1" ) )
            {
                spdlog::get(Utils::levelvol_db)->info("Stop, now: {}", std::chrono::system_clock::now());
                break;
            }

            auto token = m_redis.brpop(channel_name);
            if(!token.has_value())
            {
                spdlog::get(Utils::levelvol_db)->error("Empty token");
                continue;
            }

            on_message(token.value().second);

        }
    }

    void run_by_get( const char* channel_name)
    {
        while(true)
        {
            auto opt_stop = m_redis.get( Utils::stop_notifier );
            if( opt_stop.has_value() && ( opt_stop.value() == "1" ) )
            {
                spdlog::get(Utils::levelvol_db)->info("Stop, now: {}", std::chrono::system_clock::now());
                break;
            }

            auto token = m_redis.get(channel_name);
            if(!token.has_value())
            {
                spdlog::get(Utils::levelvol_db)->error("Empty token");
                continue;
            }

            on_message(token.value());
        }

    }
};
}