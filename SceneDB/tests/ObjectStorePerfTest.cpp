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

#include "ObjectReaders.h"
#include "ObjectWriters.h"

#include <OptiXToolkit/SceneDB/ObjectStore.h>
#include <OptiXToolkit/Util/Stopwatch.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <random>
#include <time.h>

using namespace sceneDB;

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
    std::shared_ptr<ObjectStore> m_store;

    void SetUp()
    {
        m_store = ObjectStore::getInstance( ObjectStore::Options{ "_testObjectStoreThreading" } );
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
    std::vector<size_t> objectSizes;
    if( params.fixedObjectSize )
    {
        objectSizes.resize( params.numObjects, params.fixedObjectSize );
    }
    else
    {
        // Create vector of random object sizes.
        fillRandom( objectSizes, params.numObjects, params.minObjectSize, params.maxObjectSize );
    }

    // Write the objects.  The writer is scoped to ensure that it's closed properly.
    ObjectWriters writers( m_store, objectSizes, params.numThreads );
    otk::Stopwatch time;
    writers.write();

    // Report writer stats.
    double elapsed     = time.elapsed();
    float  writeSizeMB = std::accumulate( objectSizes.begin(), objectSizes.end(), 0UL ) / ( 1024 * 1024.f );
    printf( "%i %g #write, size=%zu\n", params.numThreads, writeSizeMB / elapsed, objectSizes[0] );

    // Construct ObjectReaders, which reads the object metadata file.
    otk::Stopwatch metadataTimer;
    ObjectReaders  readers( m_store, objectSizes, /*validateData=*/params.validateData, params.numThreads );

    // Print object metadata read stats.
    double metadataTime   = metadataTimer.elapsed();
    float  metadataSizeMB = objectSizes.size() * sizeof( sceneDB::ObjectMetadata ) / ( 1024 * 1024.f );

    // Read the objects, validating their contents.
    otk::Stopwatch dataTimer;
    readers.read();

    // Print object data read stats.
    double dataTime    = dataTimer.elapsed();
    float  readSizeMB = std::accumulate( objectSizes.begin(), objectSizes.end(), 0UL ) / ( 1024 * 1024.f );
    printf( "%i %g #read, size=%zu\n", params.numThreads, readSizeMB / dataTime, objectSizes[0] );
}

unsigned int g_maxThreads = std::thread::hardware_concurrency();

std::vector<TestParams> g_params;
int initParams()
{
#if 1
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

INSTANTIATE_TEST_SUITE_P( ThreadingTests, TestObjectStoreThreading, testing::ValuesIn( g_params ) );
