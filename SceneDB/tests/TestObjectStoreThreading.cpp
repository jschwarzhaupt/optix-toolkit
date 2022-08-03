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

#include "ObjectInfo.h"

#include <OptiXToolkit/SceneDB/ObjectStore.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <random>
#include <thread>
#include <time.h>

// #define PERF_TESTING
#ifdef PERF_TESTING
#define PRINTF_INFO
#define PRINTF_STATS printf
#else
#define PRINTF_INFO printf
#define PRINTF_STATS
#endif

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
    ObjectWriters( const ObjectStore&         store,
                   const std::vector<size_t>& objectSizes,
                   unsigned int               numThreads = std::thread::hardware_concurrency() )
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
        Stopwatch time;

        // Create the ObjectStoreWriter.
        m_writer = m_store.create();
        
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

        double elapsed = time.elapsed();
        float totalSizeMB = std::accumulate(m_objectSizes.begin(), m_objectSizes.end(), 0UL) / (1024 * 1024.f);
        PRINTF_INFO( "Wrote %g MB in %g msec (%g MB/s)\n", totalSizeMB, elapsed * 1000.0, totalSizeMB / elapsed );
        PRINTF_STATS( "%zu %g #write, size=%zu\n", m_threads.size(), totalSizeMB / elapsed, m_objectSizes[0] );
    }

  private:
    const ObjectStore&                 m_store;
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

class ObjectReaders
{
  public:
    ObjectReaders( const ObjectStore&         store,
                   const std::vector<size_t>& objectSizes,
                   bool                       validateData,
                   unsigned int               numThreads = std::thread::hardware_concurrency() )
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
    }

    void read()
    {
        // Create the ObjectStoreReader.
        Stopwatch indexTimer;
        m_reader = m_store.read();

        // Print object index read stats.
        double indexTime = indexTimer.elapsed();
        float indexSizeMB = m_objectSizes.size() * sizeof( ObjectInfo ) / (1024 * 1024.f);
        PRINTF_INFO( "Object index: read %g MB (%zu entries) in %g msec (%g MB/s)\n", indexSizeMB, m_objectSizes.size(),
                     indexTime * 1000.0, indexSizeMB / indexTime );
        
        // Start threads.
        Stopwatch dataTimer;
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

        // Destroy the ObjectStoreReader.
        m_reader.reset();

        // Print object data read stats.
        double dataTime = dataTimer.elapsed();
        float totalSizeMB = std::accumulate( m_objectSizes.begin(), m_objectSizes.end(), 0UL ) / ( 1024 * 1024.f );
        PRINTF_INFO( "Object data: read %g MB in %g msec (%g MB/s)\n", totalSizeMB, dataTime * 1000.0, totalSizeMB / dataTime );
        PRINTF_STATS( "%zu %g #read, size=%zu\n", m_threads.size(), totalSizeMB / dataTime, m_objectSizes[0] );
    }

  private:
    const ObjectStore&                 m_store;
    const std::vector<size_t>&         m_objectSizes;
    bool                               m_validateData;
    std::shared_ptr<ObjectStoreReader> m_reader;
    std::vector<std::thread>           m_threads;
    std::vector<std::vector<char>>     m_buffers;
    std::atomic<int>                   m_nextObject = 0;

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

struct TestParams
{
    unsigned int numThreads;
    size_t       numObjects;
    size_t       fixedObjectSize;
    size_t       minObjectSize;
    size_t       maxObjectSize;
    bool         validateData;
};

class TestObjectStoreThreading : public testing::TestWithParam<TestParams>
{
  public:
    std::unique_ptr<ObjectStore> m_store;

    void SetUp()
    {
        m_store.reset( new ObjectStore( "_testObjectStoreThreading" ) );
    }

    void TearDown()
    {
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
};

TEST_P(TestObjectStoreThreading, TestThreadedWrite)
{
    const TestParams& params = GetParam();

    // Create vector of object sizes.
    std::vector<size_t> sizes;
    if( params.fixedObjectSize )
    {
        PRINTF_INFO( "numThreads = %i, numObjects=%zu, fixedObjectSize=%zu\n", params.numThreads, params.numObjects,
                     params.fixedObjectSize );
        sizes.resize( params.numObjects, params.fixedObjectSize );
    }
    else
    {
        // Create vector of random object sizes.
        PRINTF_INFO( "numThreads=%i, numObjects=%zu, minObjectSize=%zu, maxObjectSize=%zu\n", params.numThreads,
                     params.numObjects, params.minObjectSize, params.maxObjectSize );
        fillRandom( sizes, params.numObjects, params.minObjectSize, params.maxObjectSize );
    }

    // Write the objects.  The writer is scoped to ensure that it's closed properly.
    ObjectWriters writers( *m_store, sizes, params.numThreads );
    writers.write();

    // Read the objects, validating their contents.
    ObjectReaders readers( *m_store, sizes, /*validateData=*/ params.validateData, params.numThreads );
    readers.read();
}

unsigned int g_maxThreads = std::thread::hardware_concurrency();

#ifndef PERF_TESTING
std::vector<TestParams> g_params{
    // numThreads, numObjects, fixedObjectSize, minObjectSize, maxObjectSize, validataData
    {1, 128, 8 * 1024, 0, 0, true},
    {4 * g_maxThreads, 32 * 1024, 0, 1, 32 * 1024, true},
};
#else
std::vector<TestParams> g_params;
int initParams()
{
#if 0
    // Test throughput vs. object size.
    unsigned int numThreads = 1;
    for( size_t size = 64; size <= 1024; size += 64 )
    {
        // Params: numThreads, numObjects, fixedObjectSize, minObjectSize, maxObjectSize, validataData
        g_params.push_back( TestParams{numThreads, 10000, size, 0, 0, false } );
    }
    for( size_t size = 512; size <= 16 * 1024; size += 512 )
    {
        g_params.push_back( TestParams{numThreads, 10000, size, 0, 0, false} );
    }
#elif 0
    // Test 4K object throughput vs. thread count.
    for( unsigned int numThreads = 1; numThreads <= static_cast<unsigned int>( 1.5f * g_maxThreads ); ++numThreads )
    {
        g_params.push_back( TestParams{numThreads, 10000, 4 * 1024, 0, 0, false} );
    }
#else
    // Test 12K object throughput vs. thread count.
    for( unsigned int numThreads = 1; numThreads <= static_cast<unsigned int>( 1.5f * g_maxThreads ); ++numThreads )
    {
        g_params.push_back( TestParams{numThreads, 10000, 12 * 1024, 0, 0, false} );
    }
#endif
    return 0;
}
int g_staticInit = initParams();
#endif

INSTANTIATE_TEST_SUITE_P( ThreadingTests, TestObjectStoreThreading, testing::ValuesIn( g_params ) );
