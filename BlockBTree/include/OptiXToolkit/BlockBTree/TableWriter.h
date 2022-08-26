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

#include <OptiXToolkit/BlockBTree/GenericTableWriter.h>

namespace sceneDB {

template <typename Key, class Record>
class Table;  // forward declaration

/** TableWriter is a templated wrapper for GenericTableWriter.  It is used to insert fixed-sized
    records into a Table, associating them with keys of arbitrary (fixed) size.  */
template <typename Key, class Record>
class TableWriter
{
  public:
    /// Destroy writer, releasing any associated resources.
    ~TableWriter() = default;

    /// Insert a record with the specified key. Thread safe. Throws an exception if an error occurs.
    bool insert( const Key& key, const Record& record ) { return m_writer->insert( &key, &record ); }

    /// Update the record with the specified key, copying the given data to the specified offset in
    /// the record. Thread safe.  Throws an exception if an error occurs.
    bool update( const Key& key, void* data, size_t size, size_t offset )
    {
        return m_writer->update( &key, data, size, offset );
    }

    /// Perform muliple record updates, copying each DataBlock to the corresponding offset in the
    /// record.  Thread safe.  Throws an exception if an error occurs.
    bool updateV( const Key& key, DataBlock* dataBlocks, size_t* offsets, int numDataBlocks )
    {
        return m_writer->updateV( key, dataBlocks, offsets, numDataBlocks );
    }

    /// Remove record with the specified key, if any. Thread safe. Throws an exception if an error
    /// occurs.
    void remove( const Key& key ) { m_writer->remove( &key ); }

    /// Take a snapshot, flushing data to disk if necessary.  Once a snapshot has been taken,
    /// readers can use it as an immutable view of the table.  Subsequent insertions and updates are
    /// copy-on-write.
    std::shared_ptr<class Snapshot> takeSnapshot() { return m_writer->takeSnapshot(); }
    
    /// Flush any buffered data from previous operations to disk.  Data from any concurrent
    /// operations is not guaranteed to be flushed.
    void flush() { m_writer->flush(); }

  protected:
    friend class Table<Key, Record>;

    TableWriter( std::shared_ptr<GenericTableWriter> writer )
        : m_writer( std::move( writer ) )
    {
    }

  private:
    std::shared_ptr<GenericTableWriter> m_writer;
};

}  // namespace sceneDB
