#pragma once

#include <sw/redis++/redis++.h>
#include <string_view>
#include <iostream>
#include <iomanip>
#include <ctime>
#include <fstream>

#include <jsoncpp/json/json.h>
#include <messages/iv_msg.pb.h>
#include <Utils.h>
#include <TableBuilder.h>



namespace bluefin::chicago::applications
{

struct Tag_CSV {};
struct Tag_Feather {};

class Application
{

    static inline constexpr const char* uuid = "9f2042b0-bde2-4fc6-8d52-afc2224b26b5";
    sw::redis::Redis m_redis;

    char fmt_buffer[32];
    char log_buffer[256];

    std::size_t m_total_messages = 0;
    std::string m_sub_channel {}; //subscription channel
    std::string m_datadir;
    std::string m_filename;


    std::unique_ptr<TableBuilder> m_table_builder;
    // std::string m_suffix; //used for file rotation

    FileType m_filetype;
    std::string m_file_ext {};


    std::string m_redis_key_suffix;
    std::string m_redis_key_signal_for_live; //in csv file --> this is a list key, in feather file case --> this is a set/get key
    std::string m_redis_key_signal_for_hist;
    std::string m_redis_key_signal_for_py;

    int m_file_index = 100; //rotation file starting from 100 until 999

public:

    explicit Application( const char* file_suffix_key, const char* signal_key )
        : m_redis( "tcp://localhost:6379" )
        , m_redis_key_suffix( file_suffix_key )
        , m_redis_key_signal_for_live( signal_key )
        , m_redis_key_signal_for_hist( signal_key )
        , m_redis_key_signal_for_py( signal_key )
    {
        auto val = m_redis.get( m_redis_key_suffix );
        if( !val.has_value() )
        {
            m_redis.set( m_redis_key_suffix, "100" );
            // m_suffix = "A";
            m_file_index = 100;
        }
        else
        {
            // m_suffix = val.value();
            bool succ = true;
            try
            {
                m_file_index = std::stoi( val.value() );
            }
            catch( const std::invalid_argument& err )
            {
                succ = false;
            }
            catch( const std::out_of_range& err )
            {
                succ = false;
            }

            if( !succ )
            {
                m_file_index = 100;
                m_redis.set( m_redis_key_suffix, std::to_string( m_file_index ) );
            }
        }

        m_redis_key_signal_for_hist  += "-HIST";
        m_redis_key_signal_for_py  += "-PY";
    }

    ~Application()
    {
        Exit();
    }

    bool LoadConfig( const char* sub_channel, const char* data_directory, FileType filetype = FileType::CSV )
    {
        m_sub_channel = sub_channel;
        m_datadir = data_directory;
        spdlog::get( Utils::levelvol_receiver )->info( "Configuration Info, channel: {},  data folder: {}", m_sub_channel,  m_datadir );

        m_filetype = filetype;

        if( filetype == FileType::CSV )
        {
            m_file_ext = ".csv";
            return true;
        }
        else if( filetype == FileType::FEATHER )
        {
            m_file_ext = ".feather";
            return true;
        }


        return false;
    }


    const char* get_filename()
    {
        auto now = std::chrono::system_clock::now();
        auto msec = std::chrono::duration_cast<std::chrono::milliseconds>( now.time_since_epoch() ).count();
        /**
         * @brief filename would be ${dataDir}/EQUITY{CRYPTO}_{msec}.csv
         *
         */
        m_filename = m_datadir;
        m_filename.push_back( '/' );
        m_filename += m_sub_channel;
        m_filename.push_back( '_' );
        m_filename += m_redis_key_signal_for_live;
        m_filename.push_back( '_' );
        m_filename += std::to_string( m_file_index );
        m_filename += m_file_ext;

        // int next = ( m_suffix[0] - 'A' ) + 1;
        // next %= 26;
        // m_suffix[0] = 'A' + next;
        // m_redis.set( m_redis_key_suffix, m_suffix );
        ++m_file_index;
        if(m_file_index >= 1000)
        {
            m_file_index = 100;
        }
        m_redis.set( m_redis_key_suffix, std::to_string( m_file_index ) );
        return m_filename.c_str();
    }

    void on_message( std::string&& message, TableBuilder& table_builder, Tag_CSV )
    {
        std::string msg( message );
        ++m_total_messages;

        BFCHI::iv_record record;

        static Json::FastWriter json_writer;
        static Json::Value item;

        // item["Type"] = 1; //LIVE
        auto now = std::chrono::system_clock::now();
        item["Epoch"] = static_cast<Json::Value::Int64>( std::chrono::duration_cast<std::chrono::milliseconds>( now.time_since_epoch() ).count() );

        if( record.ParseFromString( msg ) )
        {
            auto msecs_sample_timestamp = record.sample_timestamp();
            auto msecs_expiration = record.props().expiration();

            switch( msecs_sample_timestamp )
            {
            case -2: //cycle end
                if( m_total_messages > 1 )
                {

                    table_builder.Finish( get_filename(), m_filetype );
                    //send filename to filename queue in redis
                    item["File"] = m_filename;
                    // m_redis.lpush( m_redis_key_signal, json_writer.write( item ) );
                    // m_redis.publish( pub_channel, json_writer.write( item ) );
                    /**
                     * @brief set two keys with same content
                     * 1. for live table
                     * 2. for historical table
                     */
                    m_redis.set( m_redis_key_signal_for_live, json_writer.write( item ) );
                    m_redis.set( m_redis_key_signal_for_hist, json_writer.write( item ) );
                    m_redis.set( m_redis_key_signal_for_py, json_writer.write( item ) );
                    spdlog::get( Utils::levelvol_receiver )->info( "Received {} messages", m_total_messages - 1 );
                    spdlog::get( Utils::levelvol_receiver )->info( "Write {} to key {}, {} and {}", m_filename, m_redis_key_signal_for_live, m_redis_key_signal_for_hist, m_redis_key_signal_for_py );
                }
                m_total_messages = 0;
                break;

            case -1: //end task
                spdlog::get( Utils::levelvol_receiver )->info( "LevelReceiver Exit." );
                m_redis.set( Utils::stop_notifier, "1" );
                exit( EXIT_SUCCESS );
                break;

            default:
                table_builder.AddRow<BFCHI::CallPut, BFCHI::CallPut::call>( record );
                break;
            }

        }
        else
        {
            spdlog::get( Utils::levelvol_receiver )->error( "Invalid message" );
        }
    }

    void on_message( std::string&& message, TableBuilder& table_builder, Tag_Feather )
    {
        std::string msg( message );
        ++m_total_messages;

        BFCHI::iv_record record;

        static Json::FastWriter json_writer;
        static Json::Value item;

        // item["Type"] = 1; //LIVE
        auto now = std::chrono::system_clock::now();
        item["Epoch"] = static_cast<Json::Value::Int64>( std::chrono::duration_cast<std::chrono::milliseconds>( now.time_since_epoch() ).count() );

        if( record.ParseFromString( msg ) )
        {
            auto msecs_sample_timestamp = record.sample_timestamp();
            auto msecs_expiration = record.props().expiration();

            switch( msecs_sample_timestamp )
            {
            case -2: //cycle end
                if( m_total_messages > 1 )
                {
                    table_builder.Finish( get_filename(), m_filetype );
                    //send filename to filename queue in redis
                    item["File"] = m_filename;
                    m_redis.set( m_redis_key_signal_for_live, json_writer.write( item ) );
                    m_redis.set( m_redis_key_signal_for_hist, json_writer.write( item ) );
                    m_redis.set( m_redis_key_signal_for_py, json_writer.write( item ) );
                    // m_redis.publish( pub_channel, json_writer.write( item ) );
                    spdlog::get( Utils::levelvol_receiver )->info( "Received {} messages", m_total_messages - 1 );
                    spdlog::get( Utils::levelvol_receiver )->info( "Write {} to key {}, {} and {}", m_filename, m_redis_key_signal_for_live, m_redis_key_signal_for_hist, m_redis_key_signal_for_py );
                }
                m_total_messages = 0;
                break;

            case -1: //end task
                spdlog::get( Utils::levelvol_receiver )->info( "LevelReceiver Exit." );
                m_redis.set( Utils::stop_notifier, "1" );
                exit( EXIT_SUCCESS );
                break;

            default:
                table_builder.AddRow<BFCHI::CallPut, BFCHI::CallPut::call>( record );
                break;
            }

        }
        else
        {
            spdlog::get( Utils::levelvol_receiver )->error( "Invalid message" );
        }
    }
    void Prepare( const char* domain )
    {
        auto homedir = Utils::get_homedir();

        auto init_logger = [&]()
        {
            std::string logdir = homedir;
            logdir += "/etc/logs";

            std::string basename = logdir;
            basename += "/levelvolreceiver";
            basename += "_";
            basename += domain;
            basename.push_back( '_' );
            basename += m_redis_key_signal_for_live;
            basename += ".log";

            Utils::SpdLoggerInit( basename.c_str(), Utils::levelvol_receiver );
        };

        try
        {
            init_logger();
        }
        catch( const std::exception& e )
        {
            std::cout << e.what() << '\n';
            exit( EXIT_FAILURE );
        }

        spdlog::get( Utils::levelvol_receiver )->info( "Domain: {}", domain );
        spdlog::get( Utils::levelvol_receiver )->info( "Live File Key: {}", m_redis_key_signal_for_live );
        spdlog::get( Utils::levelvol_receiver )->info( "Backup Hist File Key: {}", m_redis_key_signal_for_hist );
        spdlog::get( Utils::levelvol_receiver )->info( "Python Script File Key: {}", m_redis_key_signal_for_py );
        spdlog::get( Utils::levelvol_receiver )->info( "File Suffix Key: {}", m_redis_key_suffix );
        spdlog::get( Utils::levelvol_receiver )->info( "Start file suffix : {}", m_file_index );
    }

    void Start( bool expiration_in_local_time, Tag_CSV )
    {
        auto sub = m_redis.subscriber();
        m_table_builder = std::make_unique<TableBuilder>( expiration_in_local_time );
        m_table_builder->Reserve( 12000UL );

        sub.on_message( [this]( std::string channel, std::string msg )
        {
            this->on_message( std::move( msg ), *m_table_builder, Tag_CSV{} );
        } );

        spdlog::get( Utils::levelvol_receiver )->info( "Subscribe to {}, save to csv file", m_sub_channel );
        sub.subscribe( m_sub_channel );

        while( true )
        {
            try
            {
                sub.consume();
            }
            catch( const sw::redis::Error &err )
            {
                // std::cerr << "Error: " << err.what() << '\n';
                spdlog::get( Utils::levelvol_receiver )->error( "Error: {}", err.what() );
                return;
            }
        }
    }

    void Start( bool expiration_in_local_time, Tag_Feather )
    {
        auto sub = m_redis.subscriber();
        m_table_builder = std::make_unique<TableBuilder>( expiration_in_local_time );
        m_table_builder->Reserve( 12000UL );

        sub.on_message( [this]( std::string channel, std::string msg )
        {
            this->on_message( std::move( msg ), *m_table_builder, Tag_Feather{} );
        } );

        spdlog::get( Utils::levelvol_receiver )->info( "Subscribe to {}, Save to feather file", m_sub_channel );
        sub.subscribe( m_sub_channel );

        while( true )
        {
            try
            {
                sub.consume();
            }
            catch( const sw::redis::Error &err )
            {
                // std::cerr << "Error: " << err.what() << '\n';
                spdlog::get( Utils::levelvol_receiver )->error( "Error: {}", err.what() );
                return;
            }
        }
    }

    void Exit()
    {
    }
};

}