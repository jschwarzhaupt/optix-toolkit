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

#include "AppendOnlyFile.h"
#include <OptiXToolkit/Util/Exception.h>

#include <errno.h>
#include <fcntl.h>

#ifdef WIN32
#include <fileapi.h>
#else
#include <sys/uio.h>
#include <unistd.h>
#endif

namespace sceneDB {

AppendOnlyFile::AppendOnlyFile( const char* path )
{
    // File permissions
#ifdef WIN32
    m_descriptor = CreateFileA( path, GENERIC_WRITE, FILE_SHARE_READ, nullptr, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OVERLAPPED, nullptr );
    if( m_descriptor == INVALID_HANDLE_VALUE )
    {
        int code = GetLastError();
#else
    mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH;

    // Open file.
    m_descriptor = open( path, O_WRONLY | O_CREAT | O_TRUNC, mode );
    if( m_descriptor < 0 )
    {
        int code = errno;
#endif
        throw otk::Exception( "Cannot open AppendOnlyFile", code );
    }
}

AppendOnlyFile::~AppendOnlyFile()
{
#ifdef WIN32
    CloseHandle( m_descriptor );
#else
    close( m_descriptor );
#endif    
}

namespace {

void completionRoutine( DWORD dwErrorCode, DWORD dwNumberOfBytesTransfered, LPOVERLAPPED lpOverlapped )
{
    size_t expected = *( size_t* )( lpOverlapped->hEvent );
    if( dwErrorCode != 0 || dwNumberOfBytesTransfered != DWORD( expected ) )
        throw otk::Exception( "Error writing data to AppendOnlyFile" );
}

}

AppendOnlyFile::offset_t AppendOnlyFile::appendV( const DataBlock* dataBlocks, int numDataBlocks )
{
    // Calculate the object size.
    size_t size = sumDataBlockSizes( dataBlocks, numDataBlocks );

    // Reserve space for the object using an atomic add to fetch the current offset and increment
    // it by the object size.
    offset_t begin = m_offset.fetch_add( size );

    // Write the object at the reserved offset, using pwritev() to concatenate the given data blocks.
#ifdef WIN32
    size_t total_written = 0;
    for( int i = 0; i < numDataBlocks; ++i )
    {
        OTK_ASSERT_MSG( dataBlocks[ i ].size <= MAXDWORD, "Data block exceeds maximum size." );
        OVERLAPPED ol;
        ol.Internal = 0;
        ol.InternalHigh = 0;
        ol.hEvent = (HANDLE)&dataBlocks[ i ].size;
        ol.Offset = begin & 0xFFFFFFFFll;
        ol.OffsetHigh = DWORD( begin >> 32 );

        auto result = WriteFileEx( m_descriptor, dataBlocks[ i ].data, DWORD( dataBlocks[ i ].size ), &ol, (LPOVERLAPPED_COMPLETION_ROUTINE)(completionRoutine) );
        if( !result )
        {
            throw otk::Exception( "Error writing data to AppendOnlyFile", GetLastError() );
        }
        auto asyncResult = SleepEx( INFINITE, true );
        OTK_ASSERT_MSG( asyncResult == WAIT_IO_COMPLETION, "Error writing data to AppendOnlyFile." );
        total_written += dataBlocks[ i ].size;
    }
    OTK_ASSERT_MSG( total_written == size, "Error writing data to AppendOnlyFile." );
#else
    ssize_t bytesWritten = ::pwritev( m_descriptor, reinterpret_cast< const ::iovec* >( dataBlocks ), numDataBlocks, begin );
    if( bytesWritten != size )
        throw otk::Exception( "Error writing data to AppendOnlyFile" );
#endif    

    // Return the offset of the object data.
    return begin;
}

void AppendOnlyFile::flush()
{
// It's not necessary to flush OS buffers to disk, but this is how it's done.
#if 0
#ifdef WIN32
    int status = _commit( m_descriptor );
#else
    int status = fdatasync( m_descriptor );
#endif
#endif
}

}  // namespace sceneDB
