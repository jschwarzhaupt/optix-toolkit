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

#include <cstdint>
#include <cstddef>
#include <memory>
#include <vector>

namespace sceneDB {

/** GenericTableReader is a thread-safe reader for a GenericTable. */
class GenericTableReader
{
  public:
    /// Generic key pointer type.
    using KeyPtr = const void*;

    /// Generic record pointer type.
    using RecordPtr = const void*;
    
    /// Destroy reader, releasing any associated resources.
    virtual ~GenericTableReader() = default;

    /// Get the record size.
    virtual size_t getRecordSize() = 0;

    /// Set snapshot in which records are found.  (See GenericTableWriter::takeSnapshot.)
    virtual void setSnapshot( std::shared_ptr<class Snapshot> snapshot ) = 0;

    /// Get a pointer the record with the specified key.  Returns a null pointer if not found.
    /// Thread safe.  TODO: describe snapshot lifetime guarantee.
    virtual RecordPtr find( KeyPtr key ) = 0;
};

}  // namespace sceneDB
