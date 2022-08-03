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

#include <OptiXToolkit/SceneDB/ObjectStoreWriter.h>

#include <cstddef>
#include <memory>
#include <mutex>
#include <unordered_set>
#include <string>
#include <vector>

namespace otk {

/** ObjectStoreWriter is used to insert key/value pairs into an ObjectStore.  It supports concurrent
    insertions with no locking, with the proviso that an ObjectStore has only one writer, and only one
    process may write to the files for a particular object store. */
class ObjectStoreWriterImpl : public ObjectStoreWriter
{
  public:
    /// Destroy ObjectStoreWriter, closing any associated files.
    ~ObjectStoreWriterImpl() override;
    
    /// Insert an object with the specified key, concatenating the object data from multiple data
    /// blocks, each of which specifies a data pointer and size.  Thread safe.  When the ObjectStore
    /// has deduplication enabled, the insert call has no effect (returning false) if an object with
    /// the same key was previously inserted (i.e. the key is a content-based address).  Throws an
    /// exception if an error occurs.
    bool insertV( Key key, const DataBlock* dataBlocks, int numDataBlocks ) override;

    /// Insert an object with the specified key. Thread safe. When the ObjectStore has deduplication
    /// enabled, the insert call has no effect (returning false) if an object with the same key was
    /// previously inserted (i.e. the key is a content-based address).  Throws an exception if an
    /// error occurs.
    bool insert( Key key, const void* data, size_t size ) override
    {
        DataBlock dataBlock{data, size};
        return insertV( key, &dataBlock, 1 );
    }

    /// Remove any object with the specified key. Thread safe. Throws an exception if an error
    /// occurs.
    void remove( Key key ) override;

    /// Flush any buffered data from previous operations to disk.  Uses fdatasync/_commit to flush
    /// OS buffers as well.  Data from any concurrent operations is not guaranteed to be flushed.
    void flush() override;

    /// Get the options for this writer.
    const Options& getOptions() const { return m_options; }

  protected:
    friend class ObjectStoreImpl;

    /// Use ObjectStore::getWriter() to obtain an ObjectStoreWriter.
    ObjectStoreWriterImpl( const class ObjectStoreImpl& objectStore, const Options& options );

  private:
    Options                               m_options;
    std::unique_ptr<class AppendOnlyFile> m_objects;
    std::unique_ptr<class AppendOnlyFile> m_objectInfo;

    std::unordered_set<Key> m_keys;
    std::mutex m_keysMutex;
};

}  // namespace otk
