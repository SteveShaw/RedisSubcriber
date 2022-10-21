#include "Uploader.h"
// #include "DBAccess.h"
#include <MYSQL_Connector.h>
#include <BackupLiveToHist.h>
#include <Utils.h>

void InitLogger(const char* domain, const char* signal_key)
{
    using namespace bluefin::chicago::applications;
    auto redis = sw::redis::Redis("tcp://localhost:6379");
    auto get_log_dir = [&]()
    {
        std::string logdir = Utils::get_homedir();
        logdir += "/etc/logs";
        return logdir;
    };

    try
    {

        std::string basename = get_log_dir();
        basename += "/levelvoldb";
        basename.push_back('_');
        basename += domain;
        basename.push_back('_');
        basename += signal_key;
        basename += ".log";

        Utils::SpdLoggerInit( basename.c_str(), Utils::levelvol_db );
    }
    catch( const std::exception& e )
    {
        std::cout << e.what() << '\n';
        exit( EXIT_FAILURE );
    }
}

int main(int argc, char* argv[])
{

    using namespace bluefin::chicago::applications;

    if(argc != 3)
    {
        std::cout<<"command is not correct\n";
        exit(EXIT_FAILURE);
    }

    std::string domain = argv[1];
    std::string signal_key = argv[2];

    InitLogger(argv[1], argv[2]);

    if(domain == "EQUITY")
    {
        Uploader<mysql_connector, EQUITY_DB_Config> eq;
        std::thread t1([&](){
            // eq.run_by_list( signal_key.c_str() );
            eq.run_by_get(signal_key.c_str());
        });
        backup_live_to_history<EQUITY_DB_Config> bk_equity;
        std::thread t2([&](){ bk_equity.run(); });

        t1.join();
        t2.join();
    }
    else if(domain=="CRYPTO")
    {
        Uploader<mysql_connector, CRYPTO_DB_Config> cr;
        std::thread t1([&](){
            // cr.run_by_list( signal_key.c_str() );
            cr.run_by_get( signal_key.c_str() );
        });
        backup_live_to_history<CRYPTO_DB_Config> bk_crypto;
        std::thread t2([&](){ bk_crypto.run(); });

        t1.join();
        t2.join();
    }


    /**
     * @brief 127.0.0.1:6379> hget IV:APP:EQUITY filelist "BFCHI.LEVELS.EQ"
     *     
     */


    spdlog::get(Utils::levelvol_db)->info("LevelUploader Exit.");
}