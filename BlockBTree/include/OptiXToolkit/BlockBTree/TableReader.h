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

#pragma once

#include <OptiXToolkit/SceneDB/GenericTableReader.h>

namespace sceneDB {

/** TableReader is a templated wrapper for GenericTableReader. */
template<typename Key, class Record>
class TableReader
{
  public:
    /// Destroy reader, releasing any associated resources.
    virtual ~TableReader() = default;

    /// Set snapshot in which records are found.  (See TableWriter::takeSnapshot.)
    void setSnapshot( std::shared_ptr<class Snapshot> snapshot ) { m_reader->setSnapshot( snapshot ); }

    /// Get a pointer the record with the specified key.  Returns a null pointer if not found.
    /// Thread safe.  TODO: describe snapshot lifetime guarantee.
    const Record* find( const Key& key ) { return reinterpret_cast<const Record*>( m_reader->find( &key ) ); }

  protected:
    friend class Table<Key, Record>;

    TableReader( std::shared_ptr<GenericTableReader> reader )
        : m_reader( std::move( reader ) )
    {
    }

  private:
    std::shared_ptr<GenericTableReader> m_reader;
};

}  // namespace sceneDB
