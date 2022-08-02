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

#include <OptiXToolkit/SceneDB/ObjectStoreReader.h>
#include <OptiXToolkit/SceneDB/ObjectStoreWriter.h>

#include <cstdint>
#include <filesystem>
#include <memory>

namespace otk {

/** ObjectStore stores arbitrarily sized objects, each with an associated key.  It supports
    concurrent updates with no locking, with the proviso that only one process writes a particular
    object store.  Multiple processes can read the object store.  Once created, an object store 
    persists until it is destroyed or recreated. */
class ObjectStore
{
  public:
    /// The key is a 64-bit integer, which is typically a content-based address (CBA).
    using Key = uint64_t;

    /// Construct an ObjectStore.  No I/O is performed until create() or read() is called.
    /// \param directory { Path to directory for object store.  Created if necessary (the parent
    /// directory must exist). }
    ObjectStore( const char* directory );

    /// Close an ObjectStore, closing any associated files.
    ~ObjectStore();

    /// Open the object store for writing, destroying any previous contents.  Returns an
    /// ObjectStoreWriter that can be used to insert objects in the store.  Any previously created
    /// writers or readers should be destroyed before calling create().  Throws an exception if an
    /// error occurs.
    /// \param bufferSize { If greater than zero, writes are buffered.  Partial object records are
    /// never written.  The buffer is flushed when writing an object record that would overflow the
    /// buffer. }
    /// \param discardDuplicates { When true, inserting an object has no effect when its key
    /// is already stored.  This is useful when keys are content-based addresses (CBAs). }
    std::shared_ptr<ObjectStoreWriter> create( size_t bufferSize = 0, bool discardDuplicates = false ) const;

    /// Open the object store for reading.  Returns an ObjectStoreReader that can be used to read
    /// objects from the store.  The reader should be reused whenever possible, because creating a
    /// new reader can be expensive.
    /// \param pollForUpdates { If true, a thread is spawned that polls the filesystem for updates. }
    std::shared_ptr<ObjectStoreReader> read( bool pollForUpdates = false ) const;

    /// Check whether the object store has been created.
    bool exists() const;

    /// Destroy the object store, removing any associated disk files.  Any previously created
    /// writers or readers should be destroyed before calling destroy().
    void destroy();

  protected:
    friend ObjectStoreReader;
    friend ObjectStoreWriter;

    const std::filesystem::path& getDataFile() const { return m_dataFile; }
    const std::filesystem::path& getIndexFile() const { return m_indexFile; }

  private:
    std::filesystem::path m_directory;
    std::filesystem::path m_dataFile;
    std::filesystem::path m_indexFile;
};

}  // namespace otk
