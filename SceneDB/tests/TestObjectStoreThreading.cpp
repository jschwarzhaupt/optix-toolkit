
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
#include <numeric>
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
};

class TestObjectStoreThreading : public testing::TestWithParam<TestParams>
{
  public:
    std::shared_ptr<ObjectStore> m_store;

    void SetUp()
    {
        m_store = ObjectStore::createInstance( ObjectStore::Options{ "_testObjectStoreThreading" } );
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

TEST_P(TestObjectStoreThreading, TestThreading)
{
    const TestParams& params = GetParam();

    // Create vector of object sizes.
    std::vector<size_t> objectSizes;
    if( params.fixedObjectSize )
    {
        printf( "numThreads = %i, numObjects=%zu, fixedObjectSize=%zu\n", params.numThreads, params.numObjects,
                     params.fixedObjectSize );
        objectSizes.resize( params.numObjects, params.fixedObjectSize );
    }
    else
    {
        // Create vector of random object sizes.
        printf( "numThreads=%i, numObjects=%zu, minObjectSize=%zu, maxObjectSize=%zu\n", params.numThreads,
                     params.numObjects, params.minObjectSize, params.maxObjectSize );
        fillRandom( objectSizes, params.numObjects, params.minObjectSize, params.maxObjectSize );
    }

    // Write the objects.  The writer is scoped to ensure that it's closed properly.
    ObjectWriters writers( m_store, objectSizes, params.numThreads );
    otk::Stopwatch time;
    writers.write();

    // Report writer stats.
    double elapsed     = time.elapsed();
    float  writeSizeMB = std::accumulate( objectSizes.begin(), objectSizes.end(), 0UL ) / ( 1024 * 1024.f );
    printf( "Wrote %g MB in %g msec (%g MB/s)\n", writeSizeMB, elapsed * 1000.0, writeSizeMB / elapsed );

    // Construct ObjectReaders, which reads the object metadata file.
    otk::Stopwatch metadataTimer;
    ObjectStoreReader::Options options;
    options.pollForUpdates = false;
    ObjectReaders readers( m_store, options, objectSizes, /*validateData=*/true, params.numThreads );

    // Print object metadata read stats.
    double metadataTime   = metadataTimer.elapsed();
    float  metadataSizeMB = objectSizes.size() * sizeof( sceneDB::ObjectMetadata ) / ( 1024 * 1024.f );
    printf( "Object metadata: read %g MB (%zu entries) in %g msec (%g MB/s)\n", metadataSizeMB, objectSizes.size(),
                 metadataTime * 1000.0, metadataSizeMB / metadataTime );

    // Read the objects, validating their contents.
    otk::Stopwatch dataTimer;
    readers.read();

    // Print object data read stats.
    double dataTime    = dataTimer.elapsed();
    float  readSizeMB = std::accumulate( objectSizes.begin(), objectSizes.end(), 0UL ) / ( 1024 * 1024.f );
    printf( "Object data: read %g MB in %g msec (%g MB/s)\n", readSizeMB, dataTime * 1000.0, readSizeMB / dataTime );
}

unsigned int g_maxThreads = std::thread::hardware_concurrency();

std::vector<TestParams> g_params{
    // numThreads, numObjects, fixedObjectSize, minObjectSize, maxObjectSize
    {1, 128, 8 * 1024, 0, 0},
    {4 * g_maxThreads, 32 * 1024, 0, 1, 32 * 1024},
};

INSTANTIATE_TEST_SUITE_P( ThreadingTests, TestObjectStoreThreading, testing::ValuesIn( g_params ) );
