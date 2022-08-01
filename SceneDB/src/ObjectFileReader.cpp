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
#include <io.h>
#else
#include <unistd.h>
#endif

// Prefix some symbols with underscore under Windows
#ifdef WIN32
#define US( x ) _##x
#else
#define US( x ) x
#endif

namespace otk {

ObjectFileReader::ObjectFileReader( const char* path )
{
    m_descriptor = US( open )( path, US( O_RDONLY ) );
    if( m_descriptor < 0 )
    {
        throw Exception( "Cannot open AppendOnlyFile", errno );
    }
}

ObjectFileReader::~ObjectFileReader()
{
    close( m_descriptor );
}

void ObjectFileReader::read( off_t offset, size_t size, void* dest )
{
    // We assume system calls are automatically restarted when interrupted (sigacation SA_RESTART).
    ssize_t bytesRead = pread( m_descriptor, dest, size, offset );
    if( bytesRead < 0 )
        throw Exception( "pread() call failed in ObjectFileReader", errno );
    else if( bytesRead != size )
        throw Exception( "Incomplete pread() call in ObjectFileReader" );
}

} // namespace otk
