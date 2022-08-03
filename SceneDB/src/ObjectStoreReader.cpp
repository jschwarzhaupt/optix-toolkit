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

#include <OptiXToolkit/SceneDB/ObjectStoreReader.h>

#include "ObjectFileReader.h"
#include "ObjectInfoMap.h"
#include "ObjectStoreImpl.h"

#include <OptiXToolkit/Util/Exception.h>

#include <filesystem>

using path = std::filesystem::path;

namespace otk {

ObjectStoreReader::ObjectStoreReader( const ObjectStoreImpl& objectStore, const Options& options )
    : m_options( options )
{
    OTK_ASSERT_MSG( !m_options.pollForUpdates, "ObjectStoreReader polling is TBD." );

    // Open the object data file and read the object info file.
    m_objects.reset( new ObjectFileReader( objectStore.getDataFile().string().c_str() ) );
    m_objectInfo.reset( new ObjectInfoMap( objectStore.getIndexFile().string().c_str() ) );
}

ObjectStoreReader::~ObjectStoreReader()
{
}

bool ObjectStoreReader::find( Key key, void* dest, size_t destSize, size_t& resultSize )
{
    // Look up the key in the object info map.
    const ObjectInfo* info = m_objectInfo->find( key );
    if( !info )
        return false;

    // Read the object using the offset and size from the object info.
    OTK_ASSERT( destSize >= info->size );
    resultSize = info->size;
    m_objects->read( info->offset, info->size, dest );
    return true;
}

bool ObjectStoreReader::find( Key key, std::vector<char>& dest )
{
    // Look up the key in the object info map.
    const ObjectInfo* info = m_objectInfo->find( key );
    if( !info )
        false;

    // Allocate storage for the object.
    dest.clear();
    dest.resize( info->size );

    // Read the object using the offset and size from the object info.
    m_objects->read( info->offset, info->size, dest.data() );
    return true;
}

}  // namespace otk
