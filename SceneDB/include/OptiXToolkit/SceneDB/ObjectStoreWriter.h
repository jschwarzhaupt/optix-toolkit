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

class ObjectStoreWriter
{
  public:
    /// The key is a 64-bit integer, which is typically a content-based address (CBA).
    using Key = uint64_t;

    /// Construct ObjectStoreWriter.  Throws an exception if an error occurs.
    /// \param directory { Name of directory to contain object store files.  Created if necessary
    /// (using current umask).  Any existing object store files are removed. }
    /// \param bufferSize { If greater than zero, writes are buffered.  Partial object records are
    /// never written.  The buffer is flushed when writing an object record that would overflow the
    /// buffer.  (An object record includes the key and the object size.) }
    /// \param discardDuplicates { When true, the ObjectStoreWriter discards insertions for keys
    /// that are already stored, which is useful when keys are content-based addresses (CBAs). }
    explicit ObjectStoreWriter( const char* directory, size_t bufferSize = 0, bool discardDuplicates = false );

    /// Destroy the ObjectStoreWriter, closing any associated files.
    ~ObjectStoreWriter();

    /// Insert an object with the specified key. Thread safe. Throws an exception if an error
    /// occurs.
    void insert( Key key, void* data, size_t size );

    /// Insert an object with the specified key, concatenating the object data from multiple data
    /// sources, each with an associated size. Thread safe. Throws an exception if an error occurs.
    void insert( Key key, void** data, size_t* sizes, int count );

    /// Remove any object with the specified key. Thread safe. Throws an exception if an error
    /// occurs.
    void remove( Key key );

    /// Synchronize, ensuring that data from previous operations is written to disk (using
    /// fdatasync).  Data from any concurrent operations is not guaranteed to be synchronized.
    void synchronize();

  private:
    std::unique_ptr<class AppendOnlyFile> m_file;
};

}  // namespace otk
