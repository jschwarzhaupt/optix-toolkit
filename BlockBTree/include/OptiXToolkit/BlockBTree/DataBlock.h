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

#include <OptiXToolkit/Util/Exception.h>
#include <cstddef>
#include <atomic>
#include <mutex>

#ifdef WIN32
#include <malloc.h>
#endif

#include <cstring>  // for memcpy

namespace sceneDB {

/** A Block bundles a data pointer and size, plus validity. */
class DataBlock
{
public:
    DataBlock( const size_t size, const size_t alignment, const size_t index )
        : data( nullptr )
        , size( size )
        , index( index )
    {
#ifdef WIN32
        data = _aligned_malloc( size, alignment );
#else
        data = aligned_alloc( alignment, size );
#endif
        set_valid( false );
    }

    ~DataBlock()
    {
        set_valid( false );
        if( data )
        {
#ifdef WIN32
            _aligned_free( data );
#else
            free( data );
#endif
        }
    }

    /// No copy constructor
    DataBlock( const DataBlock& ) = delete;

    DataBlock& operator=( const DataBlock& o )
    {
        OTK_ASSERT( data && size == o.size );
        memcpy( data, o.data, size );
        set_valid( true );
        return *this;
    }

    void set_valid( bool is_valid ) const
    {
        valid.store( is_valid );
    }

    bool is_valid() const
    {
        return valid.load();
    }

    void* get_data()             { return data; }
    const void* get_data() const { return data; }

    const size_t    size;
    const size_t    index;

private:
    void*                       data;
    mutable std::atomic<bool>   valid;
};

} // namespace sceneDB
