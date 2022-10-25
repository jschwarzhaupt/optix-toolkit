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

#include <OptiXToolkit/BlockBTree/SimpleFile.h>
#include <OptiXToolkit/Util/Exception.h>

#include <errno.h>
#include <fcntl.h>

#ifdef WIN32
#include <windows.h>
#include <fileapi.h>
#else
#include <sys/uio.h>
#include <sys/file.h>
#include <unistd.h>
#endif

namespace sceneDB {

SimpleFile::SimpleFile( const std::filesystem::path& path, const bool request_write )
    : 
#ifdef WIN32
      m_file( INVALID_HANDLE_VALUE )
#else
      m_file( -1 )
#endif
{
    // File permissions
#ifdef WIN32
    // Try to open file for writing, if requested.
    if( request_write )
        m_file = CreateFileA( path.string().c_str(), GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ, nullptr, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OVERLAPPED, nullptr );

    if( m_file == INVALID_HANDLE_VALUE )
    {
        int code = GetLastError();
        if( request_write == false ||
            code == ERROR_SHARING_VIOLATION )
        {
            // Open file read-only if we couldn't open for writing, or writing wasn't requested.
            m_file = CreateFileA( path.string().c_str(), GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OVERLAPPED, nullptr );
            if( m_file == INVALID_HANDLE_VALUE )
                code = GetLastError();
            else
                code = 0;
        }

        if( code )
            throw otk::Exception( "Cannot open SimpleFile", code );
    }
    else
        m_writeable = true;
#else
    mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH;

    // Try to open file for writing, if requested.
    // Check that we can flock() the file exclusively. If not, then open read-only.
    m_file = open( path.c_str(), (request_write ? O_RDWR : O_RDONLY) | O_CREAT, mode );
    if( m_file < 0 )
    {
        int code = errno;
        throw otk::Exception( "Cannot open SimpleFile", code );
    }

    if( request_write )
    {
        int result = flock( m_file, LOCK_EX | LOCK_NB );
        if( !result )
            m_writeable = true;
    }
#endif
}

SimpleFile::~SimpleFile()
{
#ifdef WIN32
    CloseHandle( m_file );
#else
    close( m_file );
#endif    
}

namespace {

void emptyCompletion(DWORD e, DWORD c, OVERLAPPED* o)
{}

}

void SimpleFile::read( void* buffer, size_t size, offset_t offset ) const
{
    std::lock_guard guard( m_mutex );
#ifdef WIN32
    OTK_ASSERT( m_file != INVALID_HANDLE_VALUE );
    DWORD bytes_to_read = size;

    OVERLAPPED ol;
    ol.Internal = 0;
    ol.InternalHigh = 0;
    ol.Offset = offset & 0xFFFFFFFFll;
    ol.OffsetHigh = DWORD( offset >> 32 );
    ol.hEvent = NULL;

    auto result = ReadFileEx( m_file, buffer, bytes_to_read, &ol, emptyCompletion );
    if( !result ||
        SleepEx( INFINITE, true ) != WAIT_IO_COMPLETION )
    {
        throw otk::Exception( "Error attempting to read from file." );
    }

    DWORD bytes_read = 0;
    auto ol_res = GetOverlappedResult( m_file, &ol, &bytes_read, false );
    auto le_res = GetLastError();

    if( !ol_res ||
        bytes_read != bytes_to_read )
    {
        throw otk::Exception( "Error attempting to read from file.", le_res );
    }
#else
    OTK_ASSERT( m_file >= 0 );
    auto result = pread( m_file, buffer, size, offset );
    if( result != size )
    {
        throw otk::Exception( "Error attempting to read block from file.", errno );
    }
#endif
}

void SimpleFile::write( const void* buffer, size_t size, offset_t offset ) const
{
    std::lock_guard guard( m_mutex );

#ifdef WIN32
    OTK_ASSERT( m_file != INVALID_HANDLE_VALUE );
    DWORD bytes_to_write = size;

    OVERLAPPED ol;
    ol.Internal = 0;
    ol.InternalHigh = 0;
    ol.Offset = offset & 0xFFFFFFFFll;
    ol.OffsetHigh = DWORD( offset >> 32 );
    ol.hEvent = NULL;

    auto result = WriteFileEx( m_file, buffer, bytes_to_write, &ol, emptyCompletion );
    if( !result ||
        SleepEx( INFINITE, true ) != WAIT_IO_COMPLETION )
    {
        throw otk::Exception( "Error writing to file." );
    }

    DWORD bytes_written = 0;
    auto ol_res = GetOverlappedResult( m_file, &ol, &bytes_written, false );
    auto le_res = GetLastError();

    if( !ol_res ||
        bytes_written != bytes_to_write )
    {
        throw otk::Exception( "Error writing to file." );
    }
#else
    auto result = pwrite( m_file, buffer, size, offset );
    if( result != size )
    {
        throw otk::Exception( "Error writing to file." );
    }
#endif
}

}  // namespace sceneDB
