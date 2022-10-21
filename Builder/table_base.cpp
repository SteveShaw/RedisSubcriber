#include "table_base.h"


namespace bluefin::chicago::applications
{

void table_base::init( arrow::MemoryPool* pool, arrow::FieldVector& fields )
{
    m_table_schema = std::make_shared<arrow::Schema>( fields );

    for( const auto& field : fields )
    {
        switch( field->type()->id() )
        {
        case arrow::Type::STRING:
            m_builders.emplace_back( std::make_unique<arrow::StringBuilder>( pool ) );
            m_builders.back()->Reserve( CAP );
            break;

        case arrow::Type::FLOAT:
            m_builders.emplace_back( std::make_unique<arrow::FloatBuilder>( pool ) );
            m_builders.back()->Reserve( CAP );
            break;

        case arrow::Type::DOUBLE:
            m_builders.emplace_back( std::make_unique<arrow::DoubleBuilder>( pool ) );
            m_builders.back()->Reserve( CAP );
            break;

        case arrow::Type::INT32:
            m_builders.emplace_back( std::make_unique<arrow::Int32Builder>( pool ) );
            m_builders.back()->Reserve( CAP );
            break;

        case arrow::Type::INT64:
            m_builders.emplace_back( std::make_unique<arrow::Int64Builder>() );
            m_builders.back()->Reserve( CAP );
            break;

        default:
            break;
        }
    }

}

// void table_base::add_row( sql::ResultSet* rs )
void table_base::add_row( IRow* rs )
{
    std::size_t index = 0;
    for( const auto& field : m_table_schema->fields() )
    {
        switch( field->type()->id() )
        {
        case arrow::Type::STRING:
        {
            auto string_builder = static_cast<arrow::StringBuilder*>( m_builders[index].get() );
            string_builder->Append( rs->get_string( field->name().c_str() ) );
        }
        break;

        case arrow::Type::FLOAT:
        {
            auto float_builder = static_cast<arrow::FloatBuilder*>( m_builders[index].get() );
            float_builder->Append( rs->get_double( field->name().c_str() ) );
        }
        break;

        case arrow::Type::DOUBLE:
        {
            auto double_builder = static_cast<arrow::DoubleBuilder*>( m_builders[index].get() );
            double_builder->Append( rs->get_double( field->name().c_str() ) );
        }
        break;

        case arrow::Type::INT32:
        {
            auto int32_builder = static_cast<arrow::Int32Builder*>( m_builders[index].get() );
            int32_builder->Append( rs->get_int32( field->name().c_str() ) );
        }
        break;

        case arrow::Type::INT64:
        {
            auto int64_builder = static_cast<arrow::Int64Builder*>( m_builders[index].get() );
            int64_builder->Append( rs->get_int64( field->name().c_str() ) );
        }
        break;

        default:
            break;
        }

        ++index;
    }

}

arrow::Status table_base::finish( const char* path )
{
    arrow::ArrayVector columns( m_table_schema->fields().size() );
    for( std::size_t index = 0; index < m_builders.size(); ++index )
    {
        m_builders[index]->Finish( &columns[index] );
    }

    auto table = arrow::Table::Make( m_table_schema, columns );

    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    ARROW_ASSIGN_OR_RAISE( outfile, arrow::io::FileOutputStream::Open( path ) );
    auto status = arrow::ipc::feather::WriteTable( *table, outfile.get() );
    // auto csv_write_options = arrow::csv::WriteOptions::Defaults();
    // auto status = arrow::csv::WriteCSV( *table, csv_write_options, outfile.get() );
    outfile->Close();

    return status;

}


const char* table_base::get_schema_fp()
{
    return m_table_schema->fingerprint().c_str();
}
}