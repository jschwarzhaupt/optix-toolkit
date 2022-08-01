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

#include <OptiXToolkit/SceneDB/Buffer.h>

#include <atomic>
#include <cstddef>
#include <sys/types.h>

namespace otk {

/** A lock-free, thread-safe, append-only file.  Append operations are accomplished by extending the
    file (using lseek) and then writing the data (using pwritev).  Not multiprocess-safe (because
    lseek does not modify the file on disk). */
class AppendOnlyFile
{
  public:
    /// Construct AppendOnlyFile, creating or overwriting the specified file.  Throws an exception
    /// if an error occurs.
    explicit AppendOnlyFile( const char* path );

    /// Destory AppendOnlyFile, closing the associated file.
    ~AppendOnlyFile();

    /// Append the data from the given buffers, which are specified by structs containing a data
    /// pointer and size.  Returns the file offset of the data. Thread safe. Throws an exception if
    /// an error occurs.
    off_t appendV( const Buffer* buffers, int numBuffers );

    /// Append the given data.  Returns the file offset of the data. Thread safe. Throws an
    /// exception if an error occurs.
    off_t append( const void* data, size_t size )
    {
        Buffer buffer{data, size};
        return appendV( &buffer, 1 );
    }

    /// Flush any buffered data from previous operations to disk.  Uses fdatasync/_commit to flush
    /// OS buffers as well.  Data from any concurrent operations is not guaranteed to be flushed.
    void flush();

    /// Copying is prohibited.
    AppendOnlyFile(const AppendOnlyFile&) = delete;

    /// Assignment is prohibited.
    AppendOnlyFile& operator=(const AppendOnlyFile&) = delete;

  private:
    int                m_descriptor;
    std::atomic<off_t> m_offset{0};
};

}  // namespace otk
