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

#include "ObjectFileReader.h"

#include <OptiXToolkit/Util/Exception.h>

#include <errno.h>
#include <fcntl.h>
#include <filesystem>

#ifdef WIN32
#include <fileapi.h>
#else
#include <unistd.h>
#endif

namespace sceneDB {

ObjectFileReader::ObjectFileReader( const char* path )
{
    // File permissions
#ifdef WIN32
    m_descriptor = CreateFileA( path, GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OVERLAPPED, nullptr );
    if( m_descriptor == INVALID_HANDLE_VALUE )
    {
        int code = GetLastError();
#else
    // Open file.
    m_descriptor = open( path, O_RDONLY );
    if( m_descriptor < 0 )
    {
        int code = errno;
#endif
        throw otk::Exception( "Cannot open ObjectFile", code );
    }
}

ObjectFileReader::~ObjectFileReader()
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
        throw otk::Exception( "read() call failed in ObjectFileReader", dwErrorCode );
}

}

void ObjectFileReader::read( ObjectFileReader::offset_t offset, size_t size, void* dest )
{
    // We assume system calls are automatically restarted when interrupted (sigacation SA_RESTART).
    int code = 0;
#ifdef WIN32
    offset_t bytesRead = 0;
    offset_t _size = size;
    do
    {
        DWORD _bytesRead = 0;
        DWORD _bytesToRead = std::min<offset_t>( _size, MAXDWORD );

        OVERLAPPED ol;
        ol.Internal = 0;
        ol.InternalHigh = 0;
        ol.Offset = offset & 0xFFFFFFFFll;
        ol.OffsetHigh = DWORD( offset >> 32 );
        ol.hEvent = ( HANDLE )( &_bytesToRead );

        auto result = ReadFileEx( m_descriptor, (char*)(dest) + bytesRead, _bytesToRead, &ol, ( LPOVERLAPPED_COMPLETION_ROUTINE )( completionRoutine ) );
        if( !result )
        {
            bytesRead = -1;
            code = GetLastError();
            break;
        }
        auto asyncResult = SleepEx( INFINITE, true );
        if( asyncResult != WAIT_IO_COMPLETION )
        {
            bytesRead = -1;
            code = asyncResult;
        }

        bytesRead += _bytesToRead;
        _size     -= _bytesToRead;
        offset    += _bytesToRead;
    } while( _size > 0 );
#else
    ssize_t bytesRead = pread( m_descriptor, dest, size, offset );
    code = errno;
#endif
    if( bytesRead < 0 )
        throw otk::Exception( "read() call failed in ObjectFileReader", code );
    else if( bytesRead != size )
        throw otk::Exception( "Incomplete read() call in ObjectFileReader" );
}

} // namespace sceneDB
