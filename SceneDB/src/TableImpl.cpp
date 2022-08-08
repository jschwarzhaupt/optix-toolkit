//
// Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//  * Redistributions of source code must retain the above copyright
//    notice, this list of conditions and the following disclaimer.
//  * Redistributions in binary form must reproduce the above copyright
//    notice, this list of conditions and the following disclaimer in the
//    documentation and/or other materials provided with the distribution.
//  * Neither the name of NVIDIA CORPORATION nor the names of its
//    contributors may be used to endorse or promote products derived
//    from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS ``AS IS'' AND ANY
// EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
// PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
// EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
// PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
// PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY
// OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//

#include "TableImpl.h"

#include <OptiXToolkit/Util/Exception.h>

#include <filesystem>

using path = std::filesystem::path;

namespace sceneDB {

std::shared_ptr<Table> Table::createInstance( const char* directory, const char* tableName, size_t keySize, size_t recordSize )
{
    return std::shared_ptr<Table>( new TableImpl( directory, tableName, keySize, recordSize ) );
}

TableImpl::TableImpl( const char* directory, const char* tableName, size_t keySize, size_t recordSize )
    : m_tableName( tableName )
    , m_directory( directory )
    , m_dataFile( path( directory ) / path( tableName ).replace_extension( "dat" ) )
    , m_indexFile( path( directory ) / path( tableName ).replace_extension( "ind" ) )
{
}

bool TableImpl::exists() const
{
    return std::filesystem::exists( m_dataFile ) && std::filesystem::exists( m_indexFile );
}

std::shared_ptr<TableWriter> TableImpl::getWriter()
{
    std::unique_lock<std::mutex> lock( m_mutex );
    OTK_ASSERT_MSG( !m_reader, "Table::getReader should not be called before getWriter" );
    
    // Subsequent calls return the same writer, provided the options match.
    if( !m_writer )
    {
        // Create the specified directory if necessary. (Throws if an error occurs.)
        std::filesystem::create_directory( path( m_directory ) );

        // Create a new writer.
        m_writer.reset( new TableWriterImpl( *this ) );
    }

    return m_writer;
}

std::shared_ptr<TableReader> TableImpl::getReader()
{
    std::unique_lock<std::mutex> lock( m_mutex );
    if( !m_reader )
    {
        // Create a new reader.
        m_reader.reset( new TableReaderImpl( *this ) );
    }
    return m_reader;
}

void TableImpl::close()
{
    std::unique_lock<std::mutex> lock( m_mutex );
    m_writer.reset();
    m_reader.reset();
}

void TableImpl::destroy()
{
    close();
    // TODO: allow object store and multiple tables to coexist in the same directory.
    std::filesystem::remove_all( path( m_directory ) );
}

}  // namespace sceneDB
