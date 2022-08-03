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

#include "ObjectStoreWriterImpl.h"
#include "AppendOnlyFile.h"
#include "ObjectMetadata.h"
#include "ObjectStoreImpl.h"

#include <OptiXToolkit/Util/Exception.h>

#include <filesystem>

using path = std::filesystem::path;

namespace otk {

ObjectStoreWriter::~ObjectStoreWriter()
{
}

ObjectStoreWriterImpl::ObjectStoreWriterImpl( const ObjectStoreImpl& objectStore, const Options& options )
    : m_options( options )
{
    OTK_ASSERT_MSG( m_options.bufferSize == 0, "ObjectStoreWriterImpl buffering is TBD" );

    // Create object data and metadata files.
    m_objects.reset( new AppendOnlyFile( objectStore.getDataFile().string().c_str() ) );
    m_metadata.reset( new AppendOnlyFile( objectStore.getIndexFile().string().c_str() ) );
}

ObjectStoreWriterImpl::~ObjectStoreWriterImpl()
{
}

bool ObjectStoreWriterImpl::insertV( Key key, const DataBlock* dataBlocks, int numDataBlocks )
{
    // Optionally discard objects with duplicate keys (i.e. when key is a content-based addresses).
    if( m_options.discardDuplicates )
    {
        std::unique_lock lock( m_keysMutex );
        if( m_keys.find( key ) != m_keys.end() )
            return false;
        m_keys.insert( key );
    }

    // Append the object to the data file.
    off_t offset = m_objects->appendV( dataBlocks, numDataBlocks );
    size_t size = sumDataBlockSizes( dataBlocks, numDataBlocks );

    // Append a record to the object metadata file specifying the key, offset, and size of the object.
    ObjectMetadata metadata{ key, offset, size };
    m_metadata->append( &metadata, sizeof( ObjectMetadata ) );
    return true;
}

void ObjectStoreWriterImpl::remove( Key key )
{
    // TODO
    OTK_ASSERT_MSG( false, "ObjectStoreWriterImpl::remove is TBD" );
}

void ObjectStoreWriterImpl::flush()
{
    m_objects->flush();
    m_metadata->flush();
}

}  // namespace otk
