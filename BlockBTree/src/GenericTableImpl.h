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

#include "GenericTableReaderImpl.h"
#include "GenericTableWriterImpl.h"

#include <OptiXToolkit/BlockBTree/GenericTable.h>

#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>

namespace sceneDB {

/** GenericTableImpl implements the abstract GenericTable base class. */
class GenericTableImpl : public GenericTable
{
  public:
    /// Get an instance of Table with the given name in the specified directory.
    /// No I/O is performed until getWriter() or getReader() is called.
    GenericTableImpl( const char* directory, const char* tableName, size_t keySize, size_t recordSize, size_t recordAlignment );

    /// Close an GenericTableImpl.  The contents of the table persist until it is destroyed (via the
    /// destroy method).
    virtual ~GenericTableImpl() = default;

    /// Get a TableWriter that can be used to insert records in the store.  The Table is initialized
    /// when a writer is first created, destroying any previous contents.  Subsequent calls return
    /// the same writer.  The writer can be used concurrently by multiple threads. Throws an
    /// exception if an error occurs.
    std::shared_ptr<GenericTableWriter> getWriter( std::shared_ptr<GenericTable> table ) override;

    /// Get a TableReader that can be used to read records from the table.  The table is opened for
    /// reading when the first reader is requested.  Subsequent calls returns the same reader, which
    /// can be used concurrently by multiple threads.  getReader() should not be called before
    /// getWriter(), since creating a writer might reinitialize the table, which invalidates any
    /// readers.  Throws an exception if an error occurs.
    std::shared_ptr<GenericTableReader> getReader(std::shared_ptr<GenericTable> table) override;

    /// Get the table name.
    const std::string& getTableName() const override { return m_tableName; }

    /// Check whether the table has been initialized via getWriter().
    bool exists() const override;

    /// Close the table, releasing any reader and writer instances.
    void close() override;

    /// Destroy the table, closing it if necessary and removing any associated disk files.
    /// Any previously created writers or readers should be destroyed before calling destroy().
    void destroy() override;

  protected:
    friend class GenericTableReaderImpl;
    friend class GenericTableWriterImpl;

    const std::filesystem::path& getDataFile() const { return m_dataFile; }

    // Interim implementation uses ObjectStore.
    //std::shared_ptr<ObjectStore> getStore() const { return m_store; }

  private:
    const std::string           m_tableName;
    const std::filesystem::path m_directory;
    const std::filesystem::path m_dataFile;

    size_t m_keySize;
    size_t m_recordSize;
    size_t m_recordAlignment;

    std::mutex m_mutex;
    std::shared_ptr<GenericTableWriterImpl> m_writer;
    std::shared_ptr<GenericTableReaderImpl> m_reader;

    // Interim implementation uses ObjectStore.
    //std::shared_ptr<ObjectStore> m_store;
};

}  // namespace sceneDB
