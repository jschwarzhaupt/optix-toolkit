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

#include "ObjectMetadata.h"

#include <memory>

namespace sceneDB {

/** ObjectMetadata is a map from Key to ObjectMetadata, providing the file offset and size of each object
    in an object store. */
class ObjectMetadataMap
{
  public:
    using Key = uint64_t;

    /// Read object metadata file, creating a map from key to ObjectMetaData.  Throws an exception if an
    /// error occurs.  
    /// \param filename { File containing ObjectMetadata records. }
    /// \param pollForUpdates { If true, a thread is spawned that polls the filesystem for
    /// updates. }
    static std::unique_ptr<ObjectMetadataMap> read( const char* filename, bool pollForUpdates = false );

    /// Find ObjectMetadata for the specified key.  Returns true if found and returns the metadata
    /// via result parameter.
    virtual bool find( Key key, ObjectMetadata* result ) const = 0;
};

}  // namespace sceneDB
