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

#include <OptiXToolkit/SceneDB/ObjectStore.h>

#include <gtest/gtest.h>

#include <thread>

using namespace otk;

class ObjectWriters
{
  public:
    ObjectWriters( std::shared_ptr<ObjectStoreWriter> writer,
                   unsigned int                       numObjects,
                   size_t                             objectSize,
                   unsigned int                       numThreads = std::thread::hardware_concurrency() )
        : m_writer( writer )
        , m_objectsRemaining( numObjects )
        , m_objectSize( objectSize )
    {
        m_threads.reserve( numThreads );
        for( unsigned int i = 0; i < numThreads; ++i )
        {
            m_threads.emplace_back( &ObjectWriters::worker, this, i + 1 );
        }
    }

    ~ObjectWriters()
    {
        for( std::thread& thread : m_threads )
        {
            thread.join();
        }
    }

  private:
    std::shared_ptr<ObjectStoreWriter> m_writer;
    std::atomic<int>                   m_objectsRemaining;
    size_t                             m_objectSize;
    std::vector<std::thread>           m_threads;

    void worker( unsigned int threadNum )
    {
        std::vector<unsigned int> buffer( m_objectSize, threadNum );
        try
        {
            for( int objectNum = m_objectsRemaining--; objectNum > 0; objectNum = m_objectsRemaining-- )
            {
                printf("%i: %i\n", threadNum, objectNum); fflush(stdout); // XXX
                m_writer->insert( objectNum, buffer.data(), buffer.size() );
            }
        }
        catch( const std::exception& e )
        {
            std::cerr << "Error: " << e.what() << std::endl;
            std::terminate();
        }
    }
};

class TestObjectStoreThreading : public testing::Test
{
  public:
    std::unique_ptr<ObjectStore> m_store;
    std::shared_ptr<ObjectStoreWriter> m_writer;

    void SetUp()
    {
        m_store.reset( new ObjectStore( "_testObjectStoreThreading" ) );
        m_writer = m_store->create();
    }

    void TearDown()
    {
        m_writer.reset();
        m_store->destroy();
        m_store.reset();
    }
};

TEST_F(TestObjectStoreThreading, TestThreading)
{
    ObjectWriters writers( m_writer, 128, 32 * 1024, 4 * std::thread::hardware_concurrency() );
    // TODO: validate the object store.
}
