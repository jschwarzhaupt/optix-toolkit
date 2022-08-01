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

#include "FileReader.h"
#include "ObjectInfoMap.h"

#include <OptiXToolkit/SceneDB/ObjectStoreReader.h>
#include <OptiXToolkit/Util/Exception.h>

#include <filesystem>

using path = std::filesystem::path;

namespace otk {

ObjectStoreReader::ObjectStoreReader( const char* directory, bool pollForUpdates )
{
    OTK_ASSERT_MSG( !pollForUpdates, "ObjectStoreReader polling is TBD." );

    // Open the object data file and read the object info file.  The filenames must agree with the
    // ObjectStoreWriter.
    m_objects.reset( new FileReader( ( path( directory ) / "objects.dat" ).string().c_str() ) );
    m_objectInfo.reset( new ObjectInfoMap( ( path( directory ) / "objectInfo.dat" ).string().c_str() ) );
}

ObjectStoreReader::~ObjectStoreReader()
{
}

bool ObjectStoreReader::find( Key key, void* buffer, size_t bufferSize, size_t& resultSize )
{
    // Look up the key in the object info map.
    const ObjectInfo* info = m_objectInfo->find( key );
    if( !info )
        return false;

    // Read the object using the offset and size from the object info.
    OTK_ASSERT( bufferSize >= info->size );
    resultSize = info->size;
    m_objects->read( info->offset, info->size, buffer );
    return true;
}

bool ObjectStoreReader::find( Key key, std::vector<char>& buffer )
{
    // Look up the key in the object info map.
    const ObjectInfo* info = m_objectInfo->find( key );
    if( !info )
        false;

    // Allocate storage for the object.
    buffer.clear();
    buffer.resize( info->size );

    // Read the object using the offset and size from the object info.
    m_objects->read( info->offset, info->size, buffer.data() );
    return true;
}

}  // namespace otk
