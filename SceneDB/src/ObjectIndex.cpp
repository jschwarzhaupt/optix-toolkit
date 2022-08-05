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

#include "ObjectIndex.h"

#include <OptiXToolkit/Util/Exception.h>

#include <cstdint>
#include <mutex>
#include <unordered_map>

namespace sceneDB {

class ObjectMetadataMap
{
  public:
    using Key = uint64_t;

    virtual void erase( Key key ) { m_map.erase( key ); }

    virtual void insert( Key key, const ObjectMetadata& metadata ) { m_map[key] = metadata; }

    virtual bool find( Key key, ObjectMetadata* result ) const
    {
        auto it = m_map.find( key );
        if( it == m_map.end() )
            return false;

        *result = it->second;
        return true;
    }

  private:
    std::unordered_map<Key, ObjectMetadata> m_map;
};

class LockingObjectMetadataMap : public ObjectMetadataMap
{
  public:
    virtual void erase( Key key )
    {
        std::unique_lock lock( m_mutex );
        erase( key );
    }

    virtual void insert( Key key, const ObjectMetadata& metadata )
    {
        std::unique_lock lock( m_mutex );
        insert( key, metadata );
    }

    virtual bool find( Key key, ObjectMetadata* result ) const
    {
        std::unique_lock lock( m_mutex );
        return find( key, result );
    }

  private:
    mutable std::mutex m_mutex;
};


ObjectIndex::ObjectIndex( const char* filename, bool pollForUpdates )
    : m_map( pollForUpdates ? new LockingObjectMetadataMap : new ObjectMetadataMap )
    , m_file( fopen( filename, "rb" ) )
{
    OTK_ASSERT_MSG( !pollForUpdates, "ObjectMetadata polling is TBD" );
    if( m_file == nullptr )
        throw otk::Exception( ( std::string( "Error opening object metadata file: " ) + filename ).c_str() );
    readFile();
}

ObjectIndex::~ObjectIndex()
{
    fclose( m_file );
}

bool ObjectIndex::find( Key key, ObjectMetadata* result ) const
{
    return m_map->find( key, result );
}

void ObjectIndex::readFile()
{
    // Read records (using buffered I/O), updating the map.  For now we stop
    // when EOF is encountered, rather than continuing to poll for updates.
    while( readRecord() )
    {
    }
}

bool ObjectIndex::readRecord()
{
    // Read one ObjectMetadata
    ObjectMetadata metadata;
    size_t         itemsRead = fread( &metadata, sizeof( ObjectMetadata ), 1, m_file );
    if( itemsRead < 1 )
    {
        if( ferror( m_file ) )
            throw otk::Exception( "Error reading object metadata file" );
        else
            return false;  // EOF
    }

    // Special case: a sentinel offset value denotes object removal.
    if( metadata.offset == ObjectMetadata::REMOVED )
    {
        m_map->erase( metadata.key );
    }
    else
    {
        // Map key to ObjectMetadata.
        m_map->insert( metadata.key, metadata );
    }
    return true;
}

}  // namespace sceneDB
