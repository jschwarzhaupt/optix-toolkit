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

#include <OptiXToolkit/SceneDB/ObjectStore.h>

#include <cstdint>
#include <filesystem>
#include <memory>

namespace otk {

/** ObjectStoreImpl implements the abstract ObjectStore base class. */
class ObjectStoreImpl : public ObjectStore
{
  public:
    /// Construct an ObjectStoreImpl.  No I/O is performed until create() or read() is called.
    ObjectStoreImpl( const Options& options );

    /// Close an ObjectStoreImpl.  The contents of the object store persist until it is destroyed
    /// (via the destroy method).
    ~ObjectStoreImpl() override;

    /// Get an ObjectStoreWriter that can be used to insert objects in the store.  The object store
    /// is initialized when a writer is first created, destroying any previous contents.  Subsequent
    /// calls return the same writer (provided the options match, otherwise an exception is thrown).
    /// The writer can be used concurrently by multiple threads. Throws an exception if an error
    /// occurs.
    std::shared_ptr<ObjectStoreWriter> getWriter( const ObjectStoreWriter::Options& options = ObjectStoreWriter::Options() ) override;

    /// Get an an ObjectStoreReader that can be used to read objects from the store.  The object
    /// store is opened for reading when the first reader with a given set of options is requested.
    /// Subsequent calls returns the same reader, which can be used concurrently by multiple
    /// threads.  getReader() should not be called before getWriter(), since creating a writer might
    /// reinitialize the object store, which invalidates any readers.  Throws an exception if an
    /// error occurs.
    std::shared_ptr<ObjectStoreReader> getReader( const ObjectStoreReader::Options& options = ObjectStoreReader::Options() ) override;

    /// Check whether the object store has been initialized via getWriter().
    bool exists() const override;

    /// Destroy the object store, removing any associated disk files.  Any previously created
    /// writers or readers should be destroyed before calling destroy().
    void destroy() override;

  protected:
    friend ObjectStoreReader;
    friend ObjectStoreWriter;

    const std::filesystem::path& getDataFile() const { return m_dataFile; }
    const std::filesystem::path& getIndexFile() const { return m_indexFile; }

  private:
    Options               m_options;
    std::filesystem::path m_dataFile;
    std::filesystem::path m_indexFile;
};

}  // namespace otk
