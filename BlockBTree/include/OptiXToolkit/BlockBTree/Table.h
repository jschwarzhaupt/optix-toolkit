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
#pragma once

#include <OptiXToolkit/BlockBTree/GenericTable.h>
#include <OptiXToolkit/BlockBTree/TableWriter.h>
#include <OptiXToolkit/BlockBTree/TableReader.h>

namespace sceneDB {

/** Table is a templated wrapper for GenericTable. */
template <typename Key, class Record>
class Table
{
  public:
    /// Get an instance of Table with the given name in the specified directory.
    static std::shared_ptr<Table> createInstance( const char* directory, const char* tableName )
    {
        return std::shared_ptr<Table>( new Table(
            GenericTable::createInstance( directory, tableName, sizeof( Key ), sizeof( Record ), alignof( Record ) ) ) );
    }

    /// Close a Table.  The contents of the table persist until it is destroyed via the destroy() method.
    ~Table() = default;

    /// Get a TableWriter that can be used to insert records in the store.  The Table is initialized
    /// when a writer is first created, destroying any previous contents.  Subsequent calls return
    /// the same writer.  The writer can be used concurrently by multiple threads. Throws an
    /// exception if an error occurs.
    std::shared_ptr<TableWriter<Key, Record>> getWriter()
    {
        return std::shared_ptr<TableWriter<Key, Record>>( new TableWriter<Key, Record>( m_table->getWriter( m_table ) ) );
    }

    /// Get a TableReader that can be used to read records from the table.  The table is opened for
    /// reading when the first reader is requested.  Subsequent calls returns the same reader, which
    /// can be used concurrently by multiple threads.  getReader() should not be called before
    /// getWriter(), since creating a writer might reinitialize the table, which invalidates any
    /// readers.  Throws an exception if an error occurs.
    std::shared_ptr<TableReader<Key, Record>> getReader()
    {
        return std::shared_ptr<TableReader<Key, Record>>( new TableReader<Key, Record>( m_table->getReader( m_table ) ) );
    }

    /// Get the table name.
    const std::string& getTableName() const { return m_table->getTableName(); }

    /// Check whether the object store has been initialized via getWriter().
    bool exists() const { return m_table->exists(); }

    /// Close the table, releasing any reader and writer instances.
    void close() { return m_table->close(); }

    /// Destroy the table, removing any associated disk files.  Any previously created writers or
    /// readers should be destroyed before calling destroy().
    void destroy() { return m_table->destroy(); }

  private:
    std::shared_ptr<GenericTable> m_table;
    
    Table( std::shared_ptr<GenericTable> table )
        : m_table( table )
    {
    }
};

} // namespace sceneDB
