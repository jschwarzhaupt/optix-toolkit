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

#include "ObjectMetadata.h"

#include <OptiXToolkit/SceneDB/ObjectStore.h>
#include <OptiXToolkit/Util/Stopwatch.h>

#include <gtest/gtest.h>

#include <thread>

class ObjectReaders
{
  public:
    ObjectReaders( std::shared_ptr<sceneDB::ObjectStore> store,
                   const std::vector<size_t>&   objectSizes,
                   bool                         validateData,
                   unsigned int                 numThreads = std::thread::hardware_concurrency() )
        : m_store( store )
        , m_objectSizes( objectSizes )
        , m_validateData( validateData )
    {
        m_threads.reserve( numThreads );

        // Pre-allocate an object buffer for each thread, sized to the maximum object size
        m_buffers.resize( numThreads );
        size_t maxObjectSize( *std::max_element( m_objectSizes.begin(), m_objectSizes.end() ) );
        for( unsigned int threadNum = 0; threadNum < numThreads; ++threadNum )
        {
            m_buffers[threadNum].resize( maxObjectSize );
        }

        // Create the ObjectStoreReader.
        m_reader = m_store->getReader();
    }

    void read()
    {
        // Start threads.
        ASSERT_TRUE( m_threads.empty() );
        for( unsigned int i = 0; i < m_threads.capacity(); ++i )
        {
            m_threads.emplace_back( &ObjectReaders::worker, this, i );
        }

        // Wait for threads to complete.
        for( std::thread& thread : m_threads )
        {
            thread.join();
        }
    }

  private:
    std::shared_ptr<sceneDB::ObjectStore>       m_store;
    const std::vector<size_t>&                  m_objectSizes;
    bool                                        m_validateData;
    std::shared_ptr<sceneDB::ObjectStoreReader> m_reader;
    std::vector<std::thread>                    m_threads;
    std::vector<std::vector<char>>              m_buffers;
    std::atomic<int>                            m_nextObject = 0;

    void worker( unsigned int threadNum )
    {
        try
        {
            for( int objectNum = m_nextObject++; objectNum < m_objectSizes.size(); objectNum = m_nextObject++ )
            {
                // Find the object using the size as the key.  The per-thread buffer has already
                // been sized.
                size_t size = m_objectSizes[objectNum];
                std::vector<char>& buffer = m_buffers[threadNum];
                size_t actualSize;
                EXPECT_TRUE( m_reader->find( size, buffer.data(), size, actualSize ) );
                EXPECT_EQ( size, actualSize );

                // Optionally verify that the object was filled consistently.
                if (m_validateData)
                {
                    char   fillValue = buffer[0];
                    size_t count     = std::count( buffer.begin(), buffer.begin() + size, fillValue );
                    EXPECT_EQ( size, count );
                }
            }
        }
        catch( const std::exception& e )
        {
            std::cerr << "Error: " << e.what() << std::endl;
            std::terminate();
        }
    }
};

