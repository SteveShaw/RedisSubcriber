#include "Application.h"

int main(int argc, char* argv[])
{
    using namespace bluefin::chicago::applications;
    //arg 1 : EQUITy/CRYPTO
    //arg 2 : suffix key
    //arg 3 : list/(set/get key) key
    //arg 4: file type
    if(argc != 5 )
    {
        std::cout<<"command is not correct\n";
        exit(EXIT_FAILURE);
    }



    std::string instruments_type = argv[1];
    std::string suffix_key = argv[2];
    std::string signal_key = argv[3];
    std::string file_type = argv[4];

    Application app(suffix_key.c_str(), signal_key.c_str());
    app.Prepare(instruments_type.c_str());

    if(instruments_type == "EQUITY")
    {
        auto data_dir = Utils::get_homedir();
        data_dir += "/data/equity";

        if( file_type == "CSV" )
        {
            app.LoadConfig(Utils::channel_receiver_equity, data_dir.c_str());
            app.Start(true, Tag_CSV{});
        }
        else if( file_type == "FEATHER" )
        {
            app.LoadConfig(Utils::channel_receiver_equity, data_dir.c_str(), FileType::FEATHER);
            app.Start(true, Tag_Feather{});
        }
    }
    else if(instruments_type == "CRYPTO")
    {
        auto data_dir = Utils::get_homedir();
        data_dir += "/data/crypto";
        if( file_type == "CSV" )
        {
            app.LoadConfig(Utils::channel_receiver_crypto, data_dir.c_str());
            app.Start(false, Tag_CSV{});
        }
        else if( file_type == "FEATHER" )
        {
            app.LoadConfig(Utils::channel_receiver_crypto, data_dir.c_str(), FileType::FEATHER);
            app.Start(false, Tag_Feather{});
        }
    }
    
    return 0;
}
