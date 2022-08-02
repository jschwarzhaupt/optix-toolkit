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

#include <algorithm>
#include <random>
#include <thread>
#include <time.h>

using namespace otk;

class Stopwatch
{
  public:
    Stopwatch()
        : startTime( std::chrono::high_resolution_clock::now() )
    {
    }

    /// Returns the time in seconds since the Stopwatch was constructed.
    double elapsed() const
    {
        using namespace std::chrono;
        return duration_cast<duration<double>>( high_resolution_clock::now() - startTime ).count();
    }

  private:
    std::chrono::time_point<std::chrono::high_resolution_clock> startTime;
};

class ObjectWriters
{
  public:
    ObjectWriters( std::shared_ptr<ObjectStoreWriter> writer,
                   const std::vector<size_t>&         objectSizes,
                   unsigned int                       numThreads = std::thread::hardware_concurrency() )
        : m_writer( writer )
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
        Stopwatch time;
        
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

        double elapsed = time.elapsed();
        size_t totalSize = std::accumulate(m_objectSizes.begin(), m_objectSizes.end(), 0UL);
        printf( "Wrote %g MB in %g msec\n", totalSize / (1024 * 1024.f), elapsed * 1000.0 );
    }

  private:
    std::shared_ptr<ObjectStoreWriter> m_writer;
    const std::vector<size_t>&         m_objectSizes;
    std::vector<std::thread>           m_threads;
    std::vector<std::vector<char>>     m_buffers;
    std::atomic<int>                   m_nextObject = 0;

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


struct TestParams
{
    unsigned int numThreads;
    size_t numObjects;
    size_t fixedObjectSize;
    size_t minObjectSize;
    size_t maxObjectSize;
};

class TestObjectStoreThreading : public testing::TestWithParam<TestParams>
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

    // Fill vector with random size in the specified range.
    void fillRandom( std::vector<size_t>& dest, size_t count, size_t lower, size_t upper )
    {
        dest.clear();
        dest.reserve( count );

        std::mt19937 rng( time( nullptr ) );
        auto         dist = std::bind( std::uniform_int_distribution<size_t>( lower, upper ), rng );
        for( size_t i = 0; i < count; ++i )
        {
            dest.push_back( dist() );
        }
    }

    void validate( const std::vector<size_t>& sizes )
    {
        // Create object store reader.
        std::shared_ptr<ObjectStoreReader> reader = m_store->read();

        // The key for each object was its size.  Each object was filled with a constant (the thread id).
        std::vector<char> buffer;
        for( size_t size : sizes )
        {
            // Read the object, using the size as its key.
            buffer.resize( size );
            size_t actualSize;
            ASSERT_TRUE( reader->find( size, buffer.data(), buffer.size(), actualSize ) );
            EXPECT_EQ( size, actualSize );

            // Verify that the object was filled consistently.
            char   fillValue = buffer[0];
            size_t count     = std::count( buffer.begin(), buffer.end(), fillValue );
            EXPECT_EQ( size, count );
        }
    }
};

TEST_P(TestObjectStoreThreading, TestThreadedWrite)
{
    const TestParams& params = GetParam();

    // Create vector of object sizes.
    std::vector<size_t> sizes;
    if( params.fixedObjectSize )
    {
        printf( "numThreads = %i, numObjects=%zu, fixedObjectSize=%zu\n", params.numThreads, params.numObjects, params.fixedObjectSize );
        sizes.resize( params.numObjects, params.fixedObjectSize );
    }
    else
    {
        // Create vector of random object sizes.
        printf( "numThreads=%i, numObjects=%zu, minObjectSize=%zu, maxObjectSize=%zu\n", params.numThreads,
                params.numObjects, params.minObjectSize, params.maxObjectSize );
        fillRandom( sizes, params.numObjects, params.minObjectSize, params.maxObjectSize );
    }

    {
        // Write the objects.  The writer is scoped to ensure that it's closed properly.
        ObjectWriters writers( m_writer, sizes, params.numThreads );
        writers.write();
        m_writer.reset();
    }
    // Check the objects to ensure that they were written consistently.
    validate( sizes );
}

unsigned int g_maxThreads = std::thread::hardware_concurrency();

TestParams params[] = {
    // numThreads, numObjects, fixedObjectSize, minObjectSize, maxObjectSize
    {1, 128, 8 * 1024, 0, 0},
    {4 * g_maxThreads, 8 * 1024, 0, 1, 16 * 1024},
};

INSTANTIATE_TEST_SUITE_P( ThreadingTests, TestObjectStoreThreading, testing::ValuesIn( params ) );
