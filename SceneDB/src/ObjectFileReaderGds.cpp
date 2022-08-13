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

#include "ObjectFileReaderGds.h"

#include <OptiXToolkit/Util/Exception.h>

#include <errno.h>
#include <fcntl.h>
#include <filesystem>
#include <cstring>
#include <iostream>

#include <cuda_runtime.h>
#include "cufile.h"

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

namespace sceneDB {

std::string appendCuFileError( std::string msg, CUfileError_t status )
{
    const char* error = cufileop_status_error( status.err );
    return ( msg + ", CUFile Error: " + error );
}

ObjectFileReaderGds::ObjectFileReaderGds( const char* path )
{
    // Open file for reading.
    m_descriptor = US( open )( path, US( O_RDONLY | O_DIRECT ) );
    if( m_descriptor < 0 )
    {
        throw otk::Exception( ( std::string( "Failed to open file: " ) + path ).c_str() , errno );
    }

    // Open the file driver session to support GDS IO operations.
    CUfileError_t status = cuFileDriverOpen();
    if( status.err != CU_FILE_SUCCESS )
    {
        close( m_descriptor );
        throw otk::Exception( "Failed to open cuFile driver" );
    }

    // Register an open file and obtain the handle for issuing cuFile IO. (OS agnostic) 
    CUfileDescr_t cf_descr;
    memset( (void*)&cf_descr, 0, sizeof(CUfileDescr_t) );
    cf_descr.handle.fd = m_descriptor;
    cf_descr.type = CU_FILE_HANDLE_TYPE_OPAQUE_FD;
    status = cuFileHandleRegister(&m_handle, &cf_descr );
    if( status.err != CU_FILE_SUCCESS )
    {
        close( m_descriptor );
        throw otk::Exception( appendCuFileError( "Failed to register file handle", status ).c_str(), errno );
    }
}

ObjectFileReaderGds::~ObjectFileReaderGds()
{
    (void) cuFileHandleDeregister( m_handle );
    (void) cuFileDriverClose();
    close( m_descriptor );
}

void ObjectFileReaderGds::read( off_t offset, size_t size, void* dest )
{
    // Register supplied device buffer for GDS IO operations. 
    CUfileError_t status = cuFileBufRegister( dest, size, 0 );
    if( status.err != CU_FILE_SUCCESS )
    {
        std::cout << status.err << " " << status.cu_err << std::endl;
        throw otk::Exception( appendCuFileError( "Failed to register buffer", status ).c_str(), errno );
    }

    // Read size bytes from storage into supplied device buffer. 
    ssize_t bytesRead = cuFileRead( m_handle, dest, size, offset, 0 );
    if( bytesRead < 0 )
        throw otk::Exception( "ObjectFileReaderGds failed to read" );
    else if( bytesRead != size )
        throw otk::Exception( "Incomplete read in ObjectFileReaderGds" );

    // Deregister device buffer from GDS. 
    status = cuFileBufDeregister( dest );
    if( status.err != CU_FILE_SUCCESS )
    {
        throw otk::Exception( appendCuFileError( "Failed to deregister buffer", status ).c_str(), errno );
    }
}

} // namespace sceneDB
