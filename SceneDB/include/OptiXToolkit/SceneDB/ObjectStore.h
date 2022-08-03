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

#include <memory>
#include <string>

namespace otk {

/** ObjectStore stores arbitrarily sized objects, each with an associated key.  It supports
    concurrent updates with no locking, with the proviso that only one process writes a particular
    object store.  Multiple processes can read the object store.  Once created, an object store 
    persists until it is destroyed or recreated. */
class ObjectStore
{
  public:
    /// Options for configuring object store.
    struct Options
    {
        /// Path to directory containing object store files, if applicable.
        std::string directory;
    };
    
    /// Get an ObjectStore instance with the specified options.  An object store can only be written
    /// by one instance, which can be shared by multiple threads.
    static std::shared_ptr<ObjectStore> getInstance( const Options& options = Options() );

    /// Close an ObjectStore.  The contents of the object store persist until it is destroyed (via
    /// the destroy method).
    virtual ~ObjectStore();

    /// Get an ObjectStoreWriter that can be used to insert objects in the store.  The object store
    /// is initialized when a writer is first created, destroying any previous contents.  Subsequent
    /// calls return the same writer (provided the options match, otherwise an exception is thrown).
    /// The writer can be used concurrently by multiple threads. Throws an exception if an error
    /// occurs.
    virtual std::shared_ptr<ObjectStoreWriter> getWriter( const ObjectStoreWriter::Options& options = ObjectStoreWriter::Options() ) = 0;

    /// Get an an ObjectStoreReader that can be used to read objects from the store.  The object
    /// store is opened for reading when the first reader with a given set of options is requested.
    /// Subsequent calls returns the same reader, which can be used concurrently by multiple
    /// threads.  getReader() should not be called before getWriter(), since creating a writer might
    /// reinitialize the object store, which invalidates any readers.  Throws an exception if an
    /// error occurs.
    virtual std::shared_ptr<ObjectStoreReader> getReader( const ObjectStoreReader::Options& options = ObjectStoreReader::Options() ) = 0;

    /// Check whether the object store has been initialized via getWriter().
    virtual bool exists() const = 0;

    /// Destroy the object store, removing any associated disk files.  Any previously created
    /// writers or readers should be destroyed before calling destroy().
    virtual void destroy() = 0;
};

}  // namespace otk
