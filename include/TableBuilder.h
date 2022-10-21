#pragma once

#include <arrow/api.h>
#include <arrow/ipc/feather.h>
#include <arrow/status.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/compute/api.h>
// #include <arrow/ipc/writer.h>
#include <arrow/io/api.h>
#include <arrow/csv/api.h>
#include <stdexcept>

namespace bluefin::chicago::applications
{
class TableBuilder
{
    arrow::MemoryPool* m_mempool {};

    std::unique_ptr<arrow::DoubleBuilder> m_builder_askprc;
    std::unique_ptr<arrow::DoubleBuilder> m_builder_bidprc;
    std::unique_ptr<arrow::DoubleBuilder> m_builder_asksize;
    std::unique_ptr<arrow::DoubleBuilder> m_builder_bidsize;

    std::unique_ptr<arrow::DoubleBuilder> m_builder_bidvol;
    std::unique_ptr<arrow::DoubleBuilder> m_builder_askvol;
    std::unique_ptr<arrow::DoubleBuilder> m_builder_strike;
    std::unique_ptr<arrow::DoubleBuilder> m_builder_uprice;
    std::unique_ptr<arrow::DoubleBuilder> m_builder_delta;
    std::unique_ptr<arrow::DoubleBuilder> m_builder_vega;
    std::unique_ptr<arrow::DoubleBuilder> m_builder_vol;
    std::unique_ptr<arrow::TimestampBuilder> m_builder_expiration;
    std::unique_ptr<arrow::TimestampBuilder> m_builder_timestamp;

    std::unique_ptr<arrow::StringBuilder> m_builder_root;
    std::unique_ptr<arrow::StringBuilder> m_builder_callput;
    //option_id
    std::unique_ptr<arrow::StringBuilder> m_builder_id;


    //schema
    std::shared_ptr<arrow::Schema> m_table_schema;
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> m_builders;

public:

    explicit TableBuilder( bool use_local_time )
    {
        auto status = arrow::jemalloc_memory_pool( &m_mempool );
        if( !status.ok() )
        {
            throw std::runtime_error( status.message() );
        }

        //sample_timestamp
        {
            auto tm_type = std::make_shared<arrow::TimestampType>( arrow::TimeUnit::MILLI, "America/Chicago" );
            m_builder_timestamp = std::make_unique<arrow::TimestampBuilder>( tm_type, m_mempool );
        }

        //expiration
        {
            if( use_local_time )
            {
                auto tm_type = std::make_shared<arrow::TimestampType>( arrow::TimeUnit::MILLI, "America/Chicago" );
                m_builder_expiration = std::make_unique<arrow::TimestampBuilder>( tm_type, m_mempool );
            }
            else
            {
                //crypto expiration is using UTC
                auto tm_type = std::make_shared<arrow::TimestampType>( arrow::TimeUnit::MILLI, "UTC" );
                m_builder_expiration = std::make_unique<arrow::TimestampBuilder>( tm_type, m_mempool );
            }
        }

        //askprc
        m_builder_askprc = std::make_unique<arrow::DoubleBuilder>( m_mempool );
        //bidprc
        m_builder_bidprc = std::make_unique<arrow::DoubleBuilder>( m_mempool );
        //asksize
        m_builder_asksize = std::make_unique<arrow::DoubleBuilder>( m_mempool );
        //bidsize
        m_builder_bidsize = std::make_unique<arrow::DoubleBuilder>( m_mempool );

        //bidvol
        m_builder_bidvol = std::make_unique<arrow::DoubleBuilder>( m_mempool );
        //askvol
        m_builder_askvol = std::make_unique<arrow::DoubleBuilder>( m_mempool );
        //vol
        m_builder_vol = std::make_unique<arrow::DoubleBuilder>( m_mempool );
        //vega
        m_builder_vega = std::make_unique<arrow::DoubleBuilder>( m_mempool );
        //delta
        m_builder_delta = std::make_unique<arrow::DoubleBuilder>( m_mempool );

        //uprice
        m_builder_uprice = std::make_unique<arrow::DoubleBuilder>( m_mempool );
        //strike
        m_builder_strike = std::make_unique<arrow::DoubleBuilder>( m_mempool );

        //option id
        m_builder_id = std::make_unique<arrow::StringBuilder>( m_mempool );
        //callput
        m_builder_callput = std::make_unique<arrow::StringBuilder>( m_mempool );
        //root
        m_builder_root = std::make_unique<arrow::StringBuilder>( m_mempool );


        //create table schema
        std::vector<std::shared_ptr<arrow::Field>> schemas =
        {
            arrow::field( "sample_timestamp", arrow::utf8() ),
            arrow::field( "root", arrow::utf8() ),
            arrow::field( "expiration", arrow::utf8() ),
            arrow::field( "strike", arrow::float64() ),
            arrow::field( "callput", arrow::utf8() ),
            arrow::field( "option_id", arrow::utf8() ),
            arrow::field( "bidvol", arrow::float64() ),
            arrow::field( "askvol", arrow::float64() ),
            arrow::field( "bidprice", arrow::float64() ),
            arrow::field( "askprice", arrow::float64() ),
            arrow::field( "bidsize", arrow::float64() ),
            arrow::field( "asksize", arrow::float64() ),
            arrow::field( "uprice", arrow::float64() ),
            arrow::field( "delta", arrow::float64() ),
            arrow::field( "vega", arrow::float64() ),
            arrow::field( "fittedvol", arrow::float64() )
        };

        m_table_schema = std::make_shared<arrow::Schema>( schemas );
    }

    void Reserve(std::size_t max_rows = 10000)
    {
        m_builder_timestamp->Reserve(max_rows);

        m_builder_root->Reserve(max_rows);
        m_builder_expiration->Reserve(max_rows);
        m_builder_callput->Reserve(max_rows);
        m_builder_strike->Reserve(max_rows);
        m_builder_id->Reserve(max_rows);
        

        m_builder_bidprc->Reserve(max_rows);
        m_builder_askprc->Reserve(max_rows);
        m_builder_bidsize->Reserve(max_rows);
        m_builder_asksize->Reserve(max_rows);
        m_builder_uprice->Reserve(max_rows);
        
        m_builder_bidvol->Reserve(max_rows);
        m_builder_askvol->Reserve(max_rows);
        m_builder_vol->Reserve(max_rows);
        m_builder_delta->Reserve(max_rows);
        m_builder_vega->Reserve(max_rows);
    }

    template<typename Enum, Enum EnumCall, typename MSG>
    void AddRow( const MSG& msg )
    {
        m_builder_timestamp->Append( msg.sample_timestamp() );
        m_builder_root->Append( msg.props().root() );
        m_builder_expiration->Append( msg.props().expiration() );
        m_builder_strike->Append( msg.props().strike() );
        if( msg.props().callput() == EnumCall )
        {
            m_builder_callput->Append( "Call" );
        }
        else
        {
            m_builder_callput->Append( "Put" );
        }
        m_builder_id->Append( msg.props().option_id() );

        m_builder_bidvol->Append( msg.theos().bidvol() );
        m_builder_askvol->Append( msg.theos().askvol() );

        m_builder_bidprc->Append( msg.level().bidprice() );
        m_builder_askprc->Append( msg.level().askprice() );

        m_builder_bidsize->Append( msg.level().bidsize() );
        m_builder_asksize->Append( msg.level().asksize() );

        m_builder_uprice->Append( msg.level().uprice() );

        m_builder_delta->Append( msg.theos().delta() );
        m_builder_vega->Append( msg.theos().vega() );
        m_builder_vol->Append( msg.theos().model_fitted_vol() );
    }

    arrow::Status Finish(const char* filepath, FileType filetype = FileType::CSV)
    {
        arrow::ArrayVector columns( m_table_schema->fields().size() );

        {
            std::shared_ptr<arrow::Array> a;
            m_builder_timestamp->Finish( &a );
            auto options = arrow::compute::StrftimeOptions( "%Y-%m-%d %X" );
            // tm_builder.AppendValues(v);
            // tm_builder.Finish(&localtm_array);
            arrow::Datum tm_datum;
            ARROW_ASSIGN_OR_RAISE( tm_datum, arrow::compute::CallFunction( "strftime", {a}, &options ) );
            //sample_timestamp;
            columns[0] = std::move( tm_datum ).make_array();
        }

        //root
        m_builder_root->Finish( &columns[1] );

        {
            std::shared_ptr<arrow::Array> a;
            m_builder_expiration->Finish(&a);
            auto options = arrow::compute::StrftimeOptions( "%Y-%m-%d" );
            arrow::Datum ymd_datum;
            ARROW_ASSIGN_OR_RAISE( ymd_datum, arrow::compute::CallFunction( "strftime", {a}, &options ) );
            //expiration
            columns[2] = std::move( ymd_datum ).make_array();
        }

        //strike
        m_builder_strike->Finish( &columns[3] );
        //callput
        m_builder_callput->Finish( &columns[4] );
        //option id
        m_builder_id->Finish( &columns[5] );
        //bidvol
        m_builder_bidvol->Finish(&columns[6]);
        //askvol
        m_builder_askvol->Finish(&columns[7]);
        //bidprice
        m_builder_bidprc->Finish(&columns[8]);
        //askprice
        m_builder_askprc->Finish(&columns[9]);
        //bidsize
        m_builder_bidsize->Finish(&columns[10]);
        //asksize
        m_builder_asksize->Finish(&columns[11]);
        //uprice
        m_builder_uprice->Finish(&columns[12]);
        //delta
        m_builder_delta->Finish(&columns[13]);
        //vega
        m_builder_vega->Finish(&columns[14]);
        //model_fitted_vol
        m_builder_vol->Finish(&columns[15]);

        auto table = arrow::Table::Make( m_table_schema, columns );

        std::shared_ptr<arrow::io::FileOutputStream> outfile;
        ARROW_ASSIGN_OR_RAISE( outfile, arrow::io::FileOutputStream::Open( filepath ) );
        if(filetype == FileType::CSV)
        {
            auto csv_write_options = arrow::csv::WriteOptions::Defaults();
            auto status = arrow::csv::WriteCSV( *table, csv_write_options, outfile.get() );
            outfile->Close();
            return status;
        }
        else if(filetype != FileType::FEATHER)
        {
            throw std::runtime_error("Unknown Filetype");
        }

        auto status = arrow::ipc::feather::WriteTable( *table, outfile.get() );
        return status;
    }


    virtual ~TableBuilder()
    {
    }

};
}