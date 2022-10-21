#pragma once

#include <mysql-cppconn-8/jdbc/mysql_connection.h>
#include <mysql-cppconn-8/jdbc/cppconn/driver.h>
#include <mysql-cppconn-8/jdbc/cppconn/resultset.h>
#include <mysql-cppconn-8/jdbc/cppconn/statement.h>
#include <mysql-cppconn-8/jdbc/cppconn/prepared_statement.h>
#include <memory>
#include <sstream>
#include <unordered_map>
#include <string_view>
#include <Utils.h>

namespace bluefin::chicago::applications
{
class DBAccess
{
    std::string username = "Local_Python";
    std::string password = "You Suck!";
    std::string server = "qbl_mysql.servers.bluefintrading.com";

    std::unique_ptr<sql::Connection> db_conn;
    std::unique_ptr<sql::Statement> stmt;

    std::string stmtLoadFile;
    std::string stmtCallSP_BackupTable;
    std::string stmtCallSP_ClearTable;

    std::string tableName;

    // static inline const char* liveTableName = "iv_live_ML";
    // static inline const char* liveTableName = "Test_ava";
    std::string m_live_table;
    std::string m_backup_table;
    std::string m_proxy_table;

    std::string m_prefix {};

    std::string m_filelist {};

    char m_buffer[256];
    int64_t m_span_msecs {};

    std::vector<std::string> m_stmt_livesqls;
    std::vector<std::string> m_stmt_bksqls;
    std::size_t m_index_loadstmt = std::string::npos;

    //--> For testing
    // static inline const char* liveTableName = "RIOT_IV";
    // static inline const char* backupTableName = "iv_recorder_ML";

public:

    /**
     * @brief Construct a new DBAccess object
     *
     * @param schema (schema/database)
     * @param sp_before_upload (stored procedure before uploading)
     * @param sp_backup (stored procedure backup)
     */
    DBAccess( std::string schema, std::vector<std::string>&& live_sqls, std::vector<std::string>&& backup_sqls)
        : m_stmt_livesqls( std::move(live_sqls) )
        , m_stmt_bksqls( std::move( backup_sqls ) )
    {
        spdlog::get( Utils::levelvol_db )->info( "Connect to database: {}", schema );
        std::size_t index = 0;
        for(const auto& sql: m_stmt_livesqls)
        {
            if(sql.find("LOAD")!=std::string::npos)
            {
                m_index_loadstmt = index;
                spdlog::get( Utils::levelvol_db )->info( "load stmt: {}, index: {}", sql, m_index_loadstmt );
            }
            spdlog::get( Utils::levelvol_db )->info( "live sql: {}", sql );
            ++index;
        }

        for(const auto& sql: m_stmt_bksqls)
        {
            spdlog::get( Utils::levelvol_db )->info("backup sql: {}", sql);
        }


        sql::ConnectOptionsMap connection_props;
        connection_props["hostName"] = server;
        connection_props["userName"] = username;
        connection_props["password"] = password;
        connection_props["schema"] = schema;
        connection_props["OPT_LOCAL_INFILE"] = 1;

        try
        {
            db_conn.reset( get_driver_instance()->connect( connection_props ) );
            stmt.reset( db_conn->createStatement() );
            // db_conn->setAutoCommit( false );
        }
        catch( const sql::SQLException& e )
        {
            std::ostringstream err;
            err << ", error code=" << e.getErrorCode() << ", msg: " << e.what() << ", SQLState: " << e.getSQLState();
            throw std::runtime_error( err.str() );
        }
    }

    // void test()
    // {
    //     try
    //     {
    //         auto res = stmt->executeQuery( "select max(sample_timestamp) from Test_ava" );
    //         if( res->next() )
    //         {
    //             std::cout << res->getString( 1 ) << '\n';
    //             // spdlog::get(Utils::uploader)->info("maximum sample timestamp: {}", res->getString(1).c_str());
    //         }
    //     }
    //     catch( sql::SQLException& e )
    //     {
    //         using std::cout;
    //         using std::endl;
    //         cout << "# ERR: " << e.what();
    //         cout << " (MySQL error code: " << e.getErrorCode();
    //         cout << ", SQLState: " << e.getSQLState() << " )" << endl;
    //     }
    //     catch( const std::exception& e )
    //     {
    //         std::cerr << e.what() << '\n';
    //     }

    // }

    bool try_upload( const char* file_path )
    {
        std::snprintf( m_buffer, 256, m_stmt_livesqls[m_index_loadstmt].c_str(), file_path );

        try
        {

            auto t1 = std::chrono::system_clock::now();
            for(std::size_t i = 0; i < m_stmt_livesqls.size(); ++i)
            {
                if(i == m_index_loadstmt)
                {
                    spdlog::get(Utils::levelvol_db)->info("Run:  {}", m_buffer);
                    stmt->execute(m_buffer);
                }
                else
                {
                    spdlog::get(Utils::levelvol_db)->info("Run:  {}", m_stmt_livesqls[i]);
                    stmt->execute(m_stmt_livesqls[i]);
                }
            }
            std::remove(file_path);
            auto t2 = std::chrono::system_clock::now();
            spdlog::get(Utils::levelvol_db)->info("Run upload use {} msecs", std::chrono::duration_cast<std::chrono::milliseconds>( t2 - t1 ).count() );
        }
        catch( const sql::SQLException& e )
        {
            spdlog::get(Utils::levelvol_db)->error("SQLException, {}, {}:{}, Error: {}, Error Code: {}, SQLState: {}",
                        __FILE__, __FUNCTION__, __LINE__, 
                        e.what(), e.getErrorCode(), e.getSQLState());
        }
        catch( const std::exception& e )
        {
            spdlog::get(Utils::levelvol_db)->error("TryUpload Error: {}", e.what());
            return false;
        }

        return true;
    }

    void run_backup()
    {
        // std::cout << " Run backup\n ";
        try
        {
            auto t1 = std::chrono::system_clock::now();
            for(const auto& backup_sql : m_stmt_bksqls)
            {
                spdlog::get(Utils::levelvol_db)->info("Run {}", backup_sql);
                stmt->execute(backup_sql);
            }
            auto t2 = std::chrono::system_clock::now();
            spdlog::get(Utils::levelvol_db)->info("Run backup use {} msecs", std::chrono::duration_cast<std::chrono::milliseconds>( t2 - t1 ).count() );
            // stmt->execute( m_stmt_backup.c_str() );
        }
        catch( const sql::SQLException& e )
        {
            spdlog::get(Utils::levelvol_db)->error("SQLException, {}, {}:{}, Error: {}, Error Code: {}, SQLState: {}",
                        __FILE__, __FUNCTION__, __LINE__, 
                        e.what(), e.getErrorCode(), e.getSQLState());
            // std::cerr << e.what() << '\n';
        }

    }


};
}