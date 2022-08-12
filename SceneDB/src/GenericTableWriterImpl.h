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

#include <OptiXToolkit/SceneDB/GenericTableWriter.h>

#include <cstddef>
#include <memory>
#include <mutex>
#include <unordered_set>
#include <string>
#include <vector>

namespace sceneDB {

/** TableWriter is used to insert fixed-sized records into a Table, associating them with keys of
    arbitrary (fixed) size. */
class GenericTableWriterImpl : public GenericTableWriter
{
  public:
    /// Destroy GenericTableWriter, closing any associated files.
    ~GenericTableWriterImpl() override;
    
    /// Insert a record with the specified key. Thread safe. Throws an exception if an error occurs.
    bool insert( KeyPtr key, RecordPtr record ) override;

    /// Update the record with the specified key, copying the given data to the specified offset in
    /// the record. Thread safe.  Throws an exception if an error occurs.
    bool update( KeyPtr key, void* data, size_t size, size_t offset ) override;

    /// Perform muliple record updates, copying each DataBlock to the corresponding offset in the
    /// record.  Thread safe.  Throws an exception if an error occurs.
    bool updateV( KeyPtr key, DataBlock* dataBlocks, size_t* offsets, int numDataBlocks ) override;

    /// Remove record with the specified key, if any. Thread safe. Throws an exception if an error
    /// occurs.
    void remove( KeyPtr key ) override;

    /// Take a snapshot, flushing data to disk if necessary and notifying any readers of changes
    /// since the previous snapshot.  Once a snapshot has been taken, subsequent insertions and
    /// updates are copy-on-write, and readers see an immutable view of the table.
    void takeSnapshot() override;

    /// Flush any buffered data from previous operations to disk.  Data from any concurrent
    /// operations is not guaranteed to be flushed.
    void flush() override;

  protected:
    friend class GenericTableImpl;

    /// Use Table::getWriter() to obtain a GenericTableWriter.
    GenericTableWriterImpl( const class GenericTableImpl& table );

  private:
    const class GenericTableImpl& m_table;
};

}  // namespace sceneDB
