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

#include <cstddef>
#include <cstdint>
#include <memory>

namespace otk {

class ObjectStoreReader
{
  public:
    /// The key is a 64-bit integer, which is typically a content-based address (CBA).
    using Key = uint64_t;

    /// Construct ObjectStoreReader from files in the specified directory.  Optionally spawns a
    /// thread that polls the filesystem for updates. Throws an exception if an error occurs.
    ObjectStoreReader( const char* directory, bool pollForUpdates = false );

    /// Find the object with the specified key.  Returns true for success, copying the object data
    /// into the given buffer (up to the given size) and returning the object size via result parameter.
    /// Thread safe.
    bool find( Key key, void* buffer, size_t bufferSize, size_t& resultSize );

    /// Find the object with the specified key.  If successful, returns a newly allocated buffer of
    /// object data, along with its size (via result parameter).  Returns an empty pointer if the
    /// specified key was not found.  Thread safe.
    std::unique_ptr<char> find( Key key, size_t& resultSize );
};

}  // namespace otk
