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

#include <OptiXToolkit/SceneDB/DataBlock.h>
#include <OptiXToolkit/Util/Exception.h>

#include <cstdint>
#include <memory>

namespace sceneDB {

/** GenericTableWriter is used to insert fixed-sized records into a GenericTable, associating them with keys of
    arbitrary (fixed) size.  */
class GenericTableWriter
{
  public:
    /// Generic key pointer type.
    using KeyPtr = const void*;

    /// Generic record pointer type.
    using RecordPtr = const void*;

    /// Destroy writer, releasing any associated resources.
    virtual ~GenericTableWriter() = default;

    /// Insert a record with the specified key. Thread safe. Throws an exception if an error occurs.
    virtual bool insert( KeyPtr key, RecordPtr record ) = 0;

    /// Update the record with the specified key, copying the given data to the specified offset in
    /// the record. Thread safe.  Throws an exception if an error occurs.
    virtual bool update( KeyPtr key, void* data, size_t size, size_t offset ) = 0;

    /// Perform muliple record updates, copying each DataBlock to the corresponding offset in the
    /// record.  Thread safe.  Throws an exception if an error occurs.
    virtual bool updateV( KeyPtr key, DataBlock* dataBlocks, size_t* offsets, int numDataBlocks ) = 0;

    /// Remove record with the specified key, if any. Thread safe. Throws an exception if an error
    /// occurs.
    virtual void remove( KeyPtr key ) = 0;

    /// Take a snapshot, flushing data to disk if necessary and notifying any readers of changes
    /// since the previous snapshot.  Once a snapshot has been taken, subsequent insertions and
    /// updates are copy-on-write, and readers see an immutable view of the table.
    virtual void takeSnapshot() = 0;

    /// Flush any buffered data from previous operations to disk.  Data from any concurrent
    /// operations is not guaranteed to be flushed.
    virtual void flush() = 0;
};

}  // namespace sceneDB
