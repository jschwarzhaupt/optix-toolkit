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

#include <OptiXToolkit/SceneDB/ObjectStore.h>
#include <OptiXToolkit/Util/Stopwatch.h>

#include <gtest/gtest.h>

#include <thread>

class ObjectWriters
{
  public:
    ObjectWriters( std::shared_ptr<sceneDB::ObjectStore> store,
                   const std::vector<size_t>&   objectSizes,
                   unsigned int                 numThreads = std::thread::hardware_concurrency() )
        : m_store( store )
        , m_objectSizes( objectSizes )
    {
        m_threads.reserve( numThreads );

        // Pre-allocate an object buffer for each thread, sized to the maximum object size, filled with
        // the thread number (so we can detect partial/overlapping writes).
        m_buffers.resize( numThreads );
        size_t maxObjectSize( *std::max_element( m_objectSizes.begin(), m_objectSizes.end() ) );
        for( unsigned int threadNum = 0; threadNum < numThreads; ++threadNum )
        {
            unsigned char fillValue = static_cast<unsigned char>( threadNum );
            m_buffers[threadNum].resize( maxObjectSize, fillValue );
        }
    }

    void write()
    {
        // Create the ObjectStoreWriter.
        m_writer = m_store->getWriter();
        
        // Start threads.
        ASSERT_TRUE( m_threads.empty() );
        for( unsigned int i = 0; i < m_threads.capacity(); ++i )
        {
            m_threads.emplace_back( &ObjectWriters::worker, this, i );
        }

        // Wait for threads to complete.
        for( std::thread& thread : m_threads )
        {
            thread.join();
        }

        // Destroy the ObjectStoreWriter.
        m_writer.reset();
    }

  private:
    std::shared_ptr<sceneDB::ObjectStore>       m_store;
    std::shared_ptr<sceneDB::ObjectStoreWriter> m_writer;
    const std::vector<size_t>&                  m_objectSizes;
    std::vector<std::thread>                    m_threads;
    std::vector<std::vector<char>>              m_buffers;
    std::atomic<int>                            m_nextObject = 0;

    void worker( unsigned int threadNum )
    {
        try
        {
            for( int objectNum = m_nextObject++; objectNum < m_objectSizes.size(); objectNum = m_nextObject++ )
            {
                // Insert the object using the size as the key.  The per-thread buffer has already
                // been sized and filled.
                size_t size = m_objectSizes[objectNum];
                const std::vector<char>& buffer = m_buffers[threadNum];
                m_writer->insert( size, buffer.data(), size );
            }
        }
        catch( const std::exception& e )
        {
            std::cerr << "Error: " << e.what() << std::endl;
            std::terminate();
        }
    }
};

