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
#include <sys/types.h>
#include <filesystem>
#include <mutex>

namespace sceneDB {

/**
* A simple wrapper to file functionality.
* Mainly to mask system level differences.
*/
class SimpleFile
{
public:
#ifdef WIN32
      typedef long long  offset_t;
#else
      typedef off_t      offset_t;
#endif

    /// Construct SimpleFile object, creating or opening the specified file. 
    /// If the file exists, it's header is checked to validate.
    /// If request_write is true, attempts to open the file with write permissions.
    /// Note that the file may be opened in read-only mode even if write permission
    /// was requested (eg, if another process has already opened the file for writing).
    /// Throws an exception if an error occurs.
    explicit SimpleFile( const std::filesystem::path& path, const bool request_write );

    /// Destroy SimpleFile object, closing the associated file.
    ~SimpleFile();

    /// Read 'size' bytes of data from the file into 'buffer'.
    /// Start reading at 'offset' bytes from the start of the file.
    /// Throws an exception on error.
    void read( void* buffer, size_t size, offset_t offset ) const;

    /// Write 'size' bytes of data to the file from 'buffer'.
    /// Start writing at 'offset' bytes from the start of the file.
    /// Throws an exception on error.
    void write( const void* buffer, size_t size, offset_t offset ) const;

    /// Returns whether the file was opened with write access.
    bool isWriteable() const { return m_writeable;  }

    /// Closes the file and invalidates the BlockFile object.
    void close();

    /// Closes the file and erases it from disk.
    void destroy();

    bool isValid() const { return m_valid; }

    /// Copying is prohibited.
    SimpleFile(const SimpleFile&) = delete;

    /// Assignment is prohibited.
    SimpleFile& operator=(const SimpleFile&) = delete;

private:

#ifdef WIN32
    void*               m_file;
#else
    int                 m_file;
#endif

    const std::filesystem::path m_path;
    bool                        m_writeable{ false };
    mutable std::mutex          m_mutex;
    bool                        m_valid{ false };
};

}  // namespace sceneDB
