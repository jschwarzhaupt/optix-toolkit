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

#include "GenericTableImpl.h"

#include <OptiXToolkit/Util/Exception.h>

#include <filesystem>

using path = std::filesystem::path;

namespace sceneDB {

std::shared_ptr<GenericTable> GenericTable::createInstance( const char* directory, const char* tableName, size_t keySize, size_t recordSize, size_t recordAlignment )
{
    return std::shared_ptr<GenericTable>( new GenericTableImpl( directory, tableName, keySize, recordSize, recordAlignment ) );
}

GenericTableImpl::GenericTableImpl( const char* directory, const char* tableName, size_t keySize, size_t recordSize, size_t recordAlignment )
    : m_tableName( tableName )
    , m_directory( directory )
    , m_dataFile( path( directory ) / path( tableName ).replace_extension( "dat" ) )
{
}

bool GenericTableImpl::exists() const
{
    return std::filesystem::exists( m_dataFile );
}

std::shared_ptr<GenericTableWriter> GenericTableImpl::getWriter()
{
    std::unique_lock<std::mutex> lock( m_mutex );
    OTK_ASSERT_MSG( !m_reader, "GenericTable::getReader should not be called before getWriter" );
    
    // Subsequent calls return the same writer, provided the options match.
    if( !m_writer )
    {
        // Create the specified directory if necessary. (Throws if an error occurs.)
        std::filesystem::create_directory( path( m_directory ) );

        // Create a new writer.
        m_writer.reset( new GenericTableWriterImpl( *this ) );
    }

    return m_writer;
}

std::shared_ptr<GenericTableReader> GenericTableImpl::getReader()
{
    std::unique_lock<std::mutex> lock( m_mutex );
    if( !m_reader )
    {
        // Create a new reader.
        m_reader.reset( new GenericTableReaderImpl( *this ) );
    }
    return m_reader;
}

void GenericTableImpl::close()
{
    std::unique_lock<std::mutex> lock( m_mutex );
    m_writer.reset();
    m_reader.reset();
}

void GenericTableImpl::destroy()
{
    close();
    // TODO: allow object store and multiple tables to coexist in the same directory.
    std::filesystem::remove_all( path( m_directory ) );
}

}  // namespace sceneDB
