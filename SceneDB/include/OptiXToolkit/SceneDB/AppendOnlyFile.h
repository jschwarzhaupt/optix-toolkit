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
#ifndef _WIN32
#include <sys/uio.h>
#endif

namespace otk {

class AppendOnlyFile
{
  public:
    /// Construct AppendOnlyFile, creating or overwriting the specified file.  Throws
    /// an exception if an error occurs.
    explicit AppendOnlyFile( const char* path );

    /// Destory AppendOnlyFile, closing the associated file.
    ~AppendOnlyFile();

    /// Append the given data.  Returns the file offset of the data. Thread safe. Throws an
    /// exception if an error
    off_t append( const void* data, size_t size );

    /// Append the data from the given buffers, which are specified by iovec structs containing a
    /// data pointer and size.  Returns the file offset of the data. Thread safe. Throws an
    /// exception if an error occurs.
    off_t append( ::iovec* buffers, int numBuffers );

    /// Synchronize, ensuring that data from previous operations is written to disk (using
    /// fsyncdata).  Data from any concurrent operations is not guaranteed to be synchronized.
    void synchronize();

  private:
    int m_descriptor;
};

}  // namespace otk
