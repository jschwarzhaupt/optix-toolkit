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

#include "ObjectInfo.h"

#include <cstdint>
#include <unordered_map>

namespace otk {

/** ObjectInfoMap is a map from Key to ObjectInfo, providing the file offset and size of each object
    in an object store. */
class ObjectInfoMap
{
  public:
    using Key = uint64_t;

    /// Construct ObjectInfoMap, reading records from the specified file.  Throws an exception if an
    /// error occurs.  
    /// \param filename { File containing ObjectInfo records. }
    /// \param pollForUpdates { If true, a thread is spawned that polls the
    /// filesystem for updates. }
    ObjectInfoMap( const char* filename, bool pollForUpdates = false );

    /// Find ObjectInfo for the specified key.  Returns nullptr if not found.
    const ObjectInfo* find( Key key ) const
    {
        auto it = m_map.find( key );
        return it == m_map.end() ? nullptr : &it->second;
        // TODO: is it safe to return reference to an entry in an unordered_map?
    }

private:
    std::unordered_map<Key, ObjectInfo> m_map;

    void readInfo( const char* filename );
};

}  // namespace otk
