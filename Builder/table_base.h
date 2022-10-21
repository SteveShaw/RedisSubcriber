#pragma once
#include "ITable.h"

namespace bluefin::chicago::applications
{
struct table_base : public ITable
{
    std::shared_ptr<arrow::Schema> m_table_schema;
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> m_builders;

    static constexpr std::size_t CAP = 100'000;


    void init(arrow::MemoryPool* pool, arrow::FieldVector& fields);
    

    // void add_row( sql::ResultSet* rs ) override;
    void add_row(IRow* ) override;

    arrow::Status finish( const char* path ) override;
    

    const char* get_schema_fp() override;
    
};
}