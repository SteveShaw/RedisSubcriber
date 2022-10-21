#include <arrow/compute/api.h>
#include "iv_table.h"

namespace bluefin::chicago::applications
{
iv_table::iv_table( arrow::MemoryPool* pool, bool use_local_time )
{
    //create table schema
    std::vector<std::shared_ptr<arrow::Field>> fields =
    {
        arrow::field( "sample_timestamp", arrow::utf8() ),
        arrow::field( "root", arrow::utf8() ),
        arrow::field( "expiration", arrow::utf8() ),
        arrow::field( "strike", arrow::float64() ),
        arrow::field( "callput", arrow::utf8() ),
        arrow::field( "id", arrow::uint8() ),
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

    init( pool, fields );

    //specific to timestamp
    //sample_timestamp
    {
        auto tm_type = std::make_shared<arrow::TimestampType>( arrow::TimeUnit::MILLI, "America/Chicago" );
        m_builders[0].reset( nullptr );
        m_builders[0].reset( new arrow::TimestampBuilder( tm_type, pool ) );
        m_builders[0]->Reserve( CAP );
    }

    //expiration
    {
        if( use_local_time )
        {
            auto tm_type = std::make_shared<arrow::TimestampType>( arrow::TimeUnit::MILLI, "America/Chicago" );
            m_builders[2].reset( nullptr );
            m_builders[2].reset( new arrow::TimestampBuilder( tm_type, pool ) );
            m_builders[2]->Reserve( CAP );
        }
        else
        {
            //crypto expiration is using UTC
            auto tm_type = std::make_shared<arrow::TimestampType>( arrow::TimeUnit::MILLI, "UTC" );
            m_builders[2].reset( nullptr );
            m_builders[2].reset( new arrow::TimestampBuilder( tm_type, pool ) );
            m_builders[2]->Reserve( CAP );
        }
    }
}


void iv_table::add_row( const BFCHI::iv_record& msg )
{
    {
        auto builder = static_cast<arrow::TimestampBuilder*>( m_builders[0].get() );
        builder->Append( msg.sample_timestamp() );
    }

    {
        auto builder = static_cast<arrow::StringBuilder*>( m_builders[1].get() );
        builder->Append( msg.props().root() );
    }

    {
        auto builder = static_cast<arrow::TimestampBuilder*>( m_builders[2].get() );
        builder->Append( msg.props().expiration() );
    }
    //3--strike
    {
        auto builder = static_cast<arrow::DoubleBuilder*>( m_builders[3].get() );
        builder->Append( msg.props().strike() );
    }
    //4--callput
    {
        auto builder = static_cast<arrow::StringBuilder*>( m_builders[4].get() );

        if( msg.props().callput() == BFCHI::CallPut::call )
        {
            builder->Append( "Call" );
        }
        else
        {
            builder->Append( "Put" );
        }
    }
    //5--id
    {
        auto builder = static_cast<arrow::StringBuilder*>( m_builders[5].get() );
        builder->Append( msg.props().option_id() );
    }
    //6--bidvol
    {
        auto builder = static_cast<arrow::DoubleBuilder*>( m_builders[6].get() );
        builder->Append( msg.theos().bidvol() );
    }
    //7--askvol
    {
        auto builder = static_cast<arrow::DoubleBuilder*>( m_builders[7].get() );
        builder->Append( msg.theos().askvol() );
    }
    //8--bidprice
    {
        auto builder = static_cast<arrow::DoubleBuilder*>( m_builders[8].get() );
        builder->Append( msg.level().bidprice() );
    }
    //9--askprice
    {
        auto builder = static_cast<arrow::DoubleBuilder*>( m_builders[9].get() );
        builder->Append( msg.level().askprice() );
    }
    //10--bidsize
    {
        auto builder = static_cast<arrow::DoubleBuilder*>( m_builders[10].get() );
        builder->Append( msg.level().bidsize() );
    }
    //11--asksize
    {
        auto builder = static_cast<arrow::DoubleBuilder*>( m_builders[11].get() );
        builder->Append( msg.level().asksize() );
    }
    //12--uprice
    {
        auto builder = static_cast<arrow::DoubleBuilder*>( m_builders[12].get() );
        builder->Append( msg.level().uprice() );
    }
    //13--uprice
    {
        auto builder = static_cast<arrow::DoubleBuilder*>( m_builders[13].get() );
        builder->Append( msg.theos().delta() );
    }
    //14--vega
    {
        auto builder = static_cast<arrow::DoubleBuilder*>( m_builders[14].get() );
        builder->Append( msg.theos().vega() );
    }
    //15--fitted or model vol
    {
        auto builder = static_cast<arrow::DoubleBuilder*>( m_builders[15].get() );
        builder->Append( msg.theos().model_fitted_vol() );
    }

}

arrow::Status iv_table::finish( const char* csv_file )
{
    arrow::ArrayVector columns( m_table_schema->fields().size() );
    {
        std::shared_ptr<arrow::Array> a;
        m_builders[0]->Finish( &a );
        auto options = arrow::compute::StrftimeOptions( "%Y-%m-%d %X" );
        // tm_builder.AppendValues(v);
        // tm_builder.Finish(&localtm_array);
        arrow::Datum tm_datum;
        ARROW_ASSIGN_OR_RAISE( tm_datum, arrow::compute::CallFunction( "strftime", {a}, &options ) );
        //sample_timestamp;
        columns[0] = std::move( tm_datum ).make_array();
    }

    m_builders[1]->Finish( &columns[1] );

    {
        std::shared_ptr<arrow::Array> a;
        m_builders[2]->Finish( &a );
        auto options = arrow::compute::StrftimeOptions( "%Y-%m-%d" );
        arrow::Datum ymd_datum;
        ARROW_ASSIGN_OR_RAISE( ymd_datum, arrow::compute::CallFunction( "strftime", {a}, &options ) );
        //expiration
        columns[2] = std::move( ymd_datum ).make_array();
    }

    for( std::size_t index = 3; index < m_builders.size(); ++index )
    {
        m_builders[index]->Finish( &columns[index] );
    }

    auto table = arrow::Table::Make( m_table_schema, columns );

    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    ARROW_ASSIGN_OR_RAISE( outfile, arrow::io::FileOutputStream::Open( csv_file ) );
    auto status = arrow::ipc::feather::WriteTable( *table, outfile.get() );
    // auto csv_write_options = arrow::csv::WriteOptions::Defaults();
    // auto status = arrow::csv::WriteCSV( *table, csv_write_options, outfile.get() );
    outfile->Close();

    return status;
}
}