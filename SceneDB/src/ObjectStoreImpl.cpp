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

#include "ObjectStoreImpl.h"
#include "ObjectStoreReaderImpl.h"
#include "ObjectStoreWriterImpl.h"

#include <filesystem>

using path = std::filesystem::path;

namespace otk {

std::shared_ptr<ObjectStore> ObjectStore::getInstance( const Options& options )
{
    return std::shared_ptr<ObjectStore>( new ObjectStoreImpl( options ) );
}

ObjectStore::~ObjectStore()
{
}

ObjectStoreImpl::ObjectStoreImpl( const Options& options )
    : m_options( options )
    , m_dataFile( path( m_options.directory ) / "objects.dat" )
    , m_indexFile( path( m_options.directory ) / "index.dat" )
{
}

ObjectStoreImpl::~ObjectStoreImpl()
{
}

bool ObjectStoreImpl::exists() const
{
    return std::filesystem::exists( m_dataFile ) && std::filesystem::exists( m_indexFile );
}
    
std::shared_ptr<ObjectStoreWriter> ObjectStoreImpl::getWriter( const ObjectStoreWriter::Options& options )
{
    // Create the specified directory if necessary. (Throws if an error occurs.)
    std::filesystem::create_directory( path( m_options.directory ) );

    return std::shared_ptr<ObjectStoreWriter>( new ObjectStoreWriterImpl( *this, options ) );
}

std::shared_ptr<ObjectStoreReader> ObjectStoreImpl::getReader( const ObjectStoreReader::Options& options )
{
    return std::shared_ptr<ObjectStoreReader>( new ObjectStoreReaderImpl( *this, options ) );
}

void ObjectStoreImpl::destroy()
{
    std::filesystem::remove_all( path( m_options.directory ) );
}

}  // namespace otk
