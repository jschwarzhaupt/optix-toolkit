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

#include "ObjectFileWriter.h"
#include "ObjectFile.h"

#include <OptiXToolkit/Util/Exception.h>

#include <errno.h>
#include <fcntl.h>
#ifdef WIN32
#include <io.h>
#else
#include <unistd.h>
#endif

// Prefix some symbols with underscore under Windows
#ifdef WIN32
#define US(x) _ ## x
#else
#define US(x) x
#endif


namespace otk {

ObjectFileWriter::ObjectFileWriter( const char* path )
{
    // File permissions
#ifdef WIN32
    int mode = _S_IREAD | _S_IWRITE;
#else
    mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH;
#endif

    // Open file.
    m_descriptor = US(open)( path, US(O_WRONLY) | US(O_APPEND) | US(O_CREAT) | US(O_TRUNC), mode );
    if( m_descriptor < 0 )
    {
        throw Exception( "Cannot open object file", errno );
    }

    // Write header and synchronize.
    ObjectFileHeader header;
    US(write)( m_descriptor, &header, sizeof( header ) );
    synchronize();
}

ObjectFileWriter::~ObjectFileWriter()
{
    US(close)( m_descriptor );
}

void ObjectFileWriter::synchronize()
{
#ifdef WIN32
    int status = _commit(m_descriptor);
#else    
    int status = fdatasync( m_descriptor );
#endif
    if( status )
    {
        throw Exception( "Call to fdatasync failed", errno );
    }
}

}  // namespace otk
