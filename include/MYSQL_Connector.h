#pragma once
#include <mysql-cppconn-8/mysql/jdbc.h>
#include <sstream>
#include <string>
#include <chrono>

namespace bluefin::chicago::applications
{
class mysql_connector
{
    std::unique_ptr<sql::Connection> m_conn;
    std::unique_ptr<sql::Statement> m_sql_stmt;

    std::array<char, 512> m_buffer;

public:
    mysql_connector( const char *user, const char *password, const char *host, const char *db, int port = 3306 )
    {
        sql::ConnectOptionsMap props;
        props["hostName"] = host;
        props["userName"] = user;
        props["password"] = password;
        props["schema"] = db;
        props["port"] = port;
        props["OPT_LOCAL_INFILE"] = 1;

        try
        {
            m_conn.reset( get_driver_instance()->connect( props ) );
            m_sql_stmt.reset( m_conn->createStatement() );
        }
        catch( const sql::SQLException &e )
        {
            std::ostringstream err;
            err << ", error code=" << e.getErrorCode() << ", msg: " << e.what() << ", SQLState: " << e.getSQLState();
            throw std::runtime_error( err.str() );
        }
    }

    void upload_from( const char* csv_file )
    {
        /// @brief database flow
        /// @ step 1: clear temporary table (equity: IV_recorder.Temp_iv, Crypto: crypto.MARA_IV)
        /// @ step 2: load data from local csv file to temprary table
        /// @ step 3: copy from temporary table to live table (equity: IV_recorder.iv_live_ML, Crypto: crypto.iv_live_crypto)
        /// @ in step 3, the stored procedure as (CRYPTO)
        /// CREATE DEFINER=`Local_Python`@`%` PROCEDURE `copy_from_temp_to_live`()
        /// BEGIN
        /// DECLARE maxTime datetime DEFAULT NULL;
        /// START TRANSACTION;
        // SELECT MAX(sample_timestamp) FROM iv_live_crypto INTO @maxTime;
        // INSERT IGNORE INTO iv_live_crypto SELECT * FROM MARA_IV;
        // DELETE FROM iv_live_crypto WHERE sample_timestamp <= @maxTime;
        // COMMIT;
        // END 
        // @ the live table must have at least sample_timestamp and option_id as primary key
        using namespace std::string_literals;
        // auto upload_from_csv = R"(load data local INFILE '%s' REPLACE INTO TABLE MARA_IV FIELDS TERMINATED BY ',' ENCLOSED BY '"' IGNORE 1 LINES;)"s;
        auto upload_from_csv = R"(load data local INFILE '%s' REPLACE INTO TABLE temp_iv_inter FIELDS TERMINATED BY ',' ENCLOSED BY '"' IGNORE 1 LINES;)"s;
        // static char buffer[512];

        std::snprintf( m_buffer.data(), m_buffer.size(), upload_from_csv.c_str(), csv_file );
        spdlog::get(Utils::levelvol_db)->info("Load SQL: {}", m_buffer.data());

        try
        {
            m_sql_stmt->execute( "CALL clear_temp_table()" );
            m_sql_stmt->execute( m_buffer.data() );
            m_sql_stmt->execute( "CALL copy_from_temp_to_live()" );
        }
        catch( const sql::SQLException& sqlex )
        {
            throw std::runtime_error( sqlex.what() );
        }
        catch( const std::exception& e )
        {
            throw std::runtime_error( e.what() );
        }
    }

    bool save_to_history()
    {
        /**
         * @brief stored procedure (crypto)
         * CREATE DEFINER=`Local_Python`@`%` PROCEDURE `backup_from_live`()
         * BEGIN
         * START TRANSACTION;
         * INSERT IGNORE INTO BITO_IV SELECT * FROM iv_live_crypto;
         * END
         *          */
        try
        {
            m_sql_stmt->execute("CALL backup_from_live()");

            return true;
        }
        catch(const sql::SQLException& sqlex)
        {
            spdlog::get(Utils::levelvol_db)->error("SQL Error. State: {}, Msg: {}, Code: {}", sqlex.getSQLStateCStr(), sqlex.what(), sqlex.what());
        }
        
        return false;
    }
};
}