#pragma once

#include <chrono>
#include <string_view>

#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/daily_file_sink.h>

#include <unistd.h>
#include <sys/types.h>
#include <pwd.h>

namespace bluefin::chicago::applications
{

struct EQUITY_DB_Config
{
    static inline const char* user = "Local_Python";
    static inline const char* pwd = "You Suck!";
    static inline const char* host = "qbl_mysql.servers.bluefintrading.com";
    static inline const char* db = "IV_recorder";
    static inline int port = 3306;
};

struct CRYPTO_DB_Config
{
    static inline const char* user = "Local_Python";
    static inline const char* pwd = "You Suck!";
    static inline const char* host = "qbl_mysql.servers.bluefintrading.com";
    static inline const char* db = "crypto";
    static inline int port = 3306;
};

enum class FileType : uint8_t
{
    FEATHER = 0,
    CSV = 1,
    UNKNOWN = 2
};

struct Utils
{
    static inline const char* levelvol_receiver = "levelvolreceiver";
    static inline const char* levelvol_db = "levelvoldb";

    static inline const char* stop_notifier = "STOP-NOTIFIER";

    static inline const char* channel_receiver_equity = "EQSTREAM";
    static inline const char* channel_receiver_crypto = "CRSTREAM";

    static inline const char* backup_notifier = "BACKUP-ONOFF";
    static inline const char* feather_file = "FEATHER-FILE";

    static auto get_epoch_msecs()
    {
        auto now = std::chrono::system_clock::now();
        return std::chrono::duration_cast<std::chrono::milliseconds>( now.time_since_epoch() ).count();
    }

    template<typename TLogger>
    static void Log( TLogger& logger, std::string_view msg, std::string_view topic = "BLUEFIN:LOGS" )
    {
        logger.xadd( topic, "*", {std::make_pair( get_epoch_msecs(), msg )} );
    }

    static void SpdLoggerInit( const char* basename, const char* loggername )
    {
        using namespace std::chrono_literals;
        spdlog::init_thread_pool( 2048, 1 );
        auto stdout_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        // auto rotating_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>("LevelUploader.log", 1024*1024*10, 3);
        auto daily_sink = std::make_shared<spdlog::sinks::daily_file_sink_mt>( basename, 23, 59 );
        // std::vector<spdlog::sink_ptr> sinks {stdout_sink, rotating_sink};
        std::vector<spdlog::sink_ptr> sinks {stdout_sink, daily_sink};
        auto logger = std::make_shared<spdlog::async_logger>( loggername, sinks.begin(), sinks.end(),
                      spdlog::thread_pool(), spdlog::async_overflow_policy::block );
        spdlog::register_logger( logger );
        spdlog::flush_every(std::chrono::milliseconds(2000ms));
    }

    static std::string get_homedir()
    {
        auto homedir = getenv("HOME");
        if(homedir == NULL)
        {
            homedir = getpwuid(getuid())->pw_dir;
        }

        return homedir;
    }
};
}