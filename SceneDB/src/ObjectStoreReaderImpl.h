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

#include <memory>

namespace sceneDB {

/** ObjectStoreReader is a thread-safe reader for an ObjectStore.  The implementation reads a table
    of contents to create an index mapping each key to an object's size and offset in the object
    file.  Multiple records can then be read concurrently from the object file. */
class ObjectStoreReaderImpl : public ObjectStoreReader
{
  public:
    /// Destroy ObjectStoreReader, closing any associated files.
    virtual ~ObjectStoreReaderImpl();

    /// Find the object with the specified key.  Returns true for success, copying the object data
    /// into the given buffer and returning the object size via result parameter.  Throws an exception
    /// if the object size exceeds the buffer size.  Thread safe.
    bool find( Key key, void* dest, size_t destSize, size_t& resultSize ) override;

    /// Find the object with the specified key.  Returns true for success, copying the object data
    /// into the given buffer (which is resized if necessary).  Thread safe.
    bool find( Key key, std::vector<char>& dest ) override;

    /// Get the options for this reader.
    const Options& getOptions() const { return m_options; }

  protected:
    friend class ObjectStoreImpl;

    /// Use ObjectStore::read() to obtain an ObjectStoreReader.
    ObjectStoreReaderImpl( const class ObjectStoreImpl& objectStore, const Options& options );

  private:
    Options                                  m_options;
    std::unique_ptr<class ObjectFileReader>  m_objects;
    std::unique_ptr<class ObjectIndex>       m_index;
};

}  // namespace sceneDB
