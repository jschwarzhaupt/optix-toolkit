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

#include "ObjectMetadataMap.h"

#include <OptiXToolkit/Util/Exception.h>

namespace sceneDB {

ObjectMetadataMap::ObjectMetadataMap( const char* filename, bool pollForUpdates )
{
    OTK_ASSERT_MSG( !pollForUpdates, "ObjectMetadata polling is TBD" );
    readMetadata( filename );
}

void ObjectMetadataMap::readMetadata( const char* filename )
{
    // Open the object metadata file.
    FILE* file = fopen( filename, "rb" );
    if( file == nullptr )
        throw otk::Exception( ( std::string( "Error opening object metadata file: " ) + filename ).c_str() );

    // Read records (using buffered I/O), updating the map.  For now we stop
    // when EOF is encountered, rather than continuing to poll for updates.
    ObjectMetadata metadata;
    while (true)
    {
        // Read one ObjectMetadata 
        size_t itemsRead = fread( &metadata, sizeof( ObjectMetadata ), 1, file );
        if (itemsRead < 1)
        {
            if (ferror(file))
                throw otk::Exception( ( std::string( "Error reading object metadata file: " ) + filename ).c_str() );
            else
                return; // EOF

        }
        // Map key to ObjectMetadata.
        m_map[metadata.key] = metadata;
    }
}

} // namespace sceneDB
