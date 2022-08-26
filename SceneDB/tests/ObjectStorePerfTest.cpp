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
#include <OptiXToolkit/Util/Exception.h>
#include <OptiXToolkit/Util/Stopwatch.h>

#include <cstdio>
#include <fcntl.h>
#include <numeric>

using namespace sceneDB;

class ObjectStorePerfTest
{
  public:
    ObjectStorePerfTest( const char* statsFilename, bool useGds )
        : m_statsFile( fopen( statsFilename, "w" ) )
        , m_useGds( useGds )
    {
        OTK_ASSERT_MSG( m_statsFile != nullptr, "Error opening stats file" );
        fprintf( m_statsFile, "num threads,num objects,object size,write throughput,read throughput,metadata throughput\n" );
        printf( "Writing \"%s\" ", statsFilename );
        fflush(stdout);
    }

    ~ObjectStorePerfTest()
    {
        fclose( m_statsFile );
        printf(" done\n");
    }

  private:
    const unsigned int m_maxThreads = std::thread::hardware_concurrency();
    FILE*              m_statsFile;
    bool               m_useGds;

    struct Params
    {
        unsigned int numThreads;
        size_t       numObjects;
        size_t       objectSize;
        bool         pollForUpdates;
        bool         dropCaches;
    };

    void run( const Params& params )
    {
        // Create ObjectStore
        std::shared_ptr<ObjectStore> store( ObjectStore::createInstance( ObjectStore::Options{"_testObjectStorePerf"} ) );

        // Create vector of object sizes.
        std::vector<size_t> objectSizes( params.numObjects, params.objectSize );

        // Write the objects.  The writer is scoped to ensure that it's closed properly.
        std::unique_ptr<ObjectWriters> writers( new ObjectWriters( store, objectSizes, params.numThreads ) );
        otk::Stopwatch writeTimer;
        writers->write();
        double writeTime   = writeTimer.elapsed();
        writers.reset();

        // Optionally drop caches (requires root permission).
        if( params.dropCaches )
        {
            int fd = open( "/proc/sys/vm/drop_caches", O_WRONLY );
            OTK_ASSERT_MSG( fd != -1, "Must run as root to drop caches" );
            auto bytesWritten = write( fd, "3", 1 );
            OTK_ASSERT_MSG( bytesWritten == 1, "Write to drop_caches failed" );
            close( fd );
        }

        // Construct ObjectReaders, which reads the object metadata file.
        otk::Stopwatch metadataTimer;
        ObjectStoreReader::Options options;
        options.pollForUpdates = false;
        options.useGds = m_useGds;
        std::unique_ptr<ObjectReaders> readers(
            new ObjectReaders( store, options, objectSizes, /*validateData=*/false, params.numThreads ) );
        double metadataTime   = metadataTimer.elapsed();

        // Read the objects
        otk::Stopwatch dataTimer;
        readers->read();
        double dataTime = dataTimer.elapsed();
        readers.reset();

        // Print stats.
        float  objectsSizeMB = std::accumulate( objectSizes.begin(), objectSizes.end(), 0UL ) / ( 1024 * 1024.f );
        float  metadataSizeMB = objectSizes.size() * sizeof( sceneDB::ObjectMetadata ) / ( 1024 * 1024.f );
        fprintf( m_statsFile, "%i,%zu,%zu,%g,%g,%g\n", params.numThreads, objectSizes.size(), objectSizes[0],
                 objectsSizeMB / writeTime, objectsSizeMB / dataTime, metadataSizeMB / metadataTime );
        fflush( m_statsFile );
        fputc('.', stdout);
        fflush(stdout);

        // Destroy the object store.
        store->destroy();
    }

  public:
    // Test throughput vs. object size.
    void testObjectSize()
    {
        unsigned int numThreads = 1;
        for( size_t size = 128; size < 4 * 128; size += 128 )
        {
            // Params: numThreads, numObjects, objectSize, pollForUpdates, dropCaches
            run( Params{numThreads, 100000, size, false, false} );
        }
        for( size_t size = 512; size < 4 * 512; size += 512 )
        {
            // Params: numThreads, numObjects, objectSize
            run( Params{numThreads, 100000, size, false, false} );
        }
        for( size_t size = 2048; size < 4 * 2048; size += 2048 )
        {
            // Params: numThreads, numObjects, objectSize
            run( Params{ numThreads, 100000, size, false, false } );
        }
        for( size_t size = 8192; size < 4 * 8192; size += 8192 )
        {
            // Params: numThreads, numObjects, objectSize
            run( Params{ numThreads, 100000, size, false, false } );
        }
        for( size_t size = 32768; size < 4 * 32768; size += 32768 )
        {
            // Params: numThreads, numObjects, objectSize
            run( Params{ numThreads, 100000, size, false, false } );
        }
    }

    // Test 4K object throughput vs. thread count.
    void testThreads4K()
    {
        for( unsigned int numThreads = 1; numThreads <= static_cast<unsigned int>( 1.5f * m_maxThreads ); ++numThreads )
        {
            // Params: numThreads, numObjects, objectSize, pollForUpdates, dropCaches
            run( Params{numThreads, 100000, 4 * 1024, false, false} );
        }
    }

    // Test 12K object throughput vs. thread count.
    void testThreads12K()
    {
        for( unsigned int numThreads = 1; numThreads <= static_cast<unsigned int>( 1.5f * m_maxThreads ); ++numThreads )
        {
            // Params: numThreads, numObjects, objectSize, pollForUpdates, dropCaches
            run( Params{numThreads, 100000, 12 * 1024, false, false} );
        }
    }

    // Test 64K object throughput vs. thread count.
    void testThreads64K()
    {
        for( unsigned int numThreads = 1; numThreads <= static_cast< unsigned int >( 1.5f * m_maxThreads ); ++numThreads )
        {
            // Params: numThreads, numObjects, objectSize, pollForUpdates, dropCaches
            run( Params{ numThreads, 100000, 64 * 1024, false, false } );
        }
    }
    
    // Test 256K object throughput vs. thread count.
    void testThreads256K()
    {
        for( unsigned int numThreads = 1; numThreads <= static_cast< unsigned int >( 1.5f * m_maxThreads ); ++numThreads )
        {
            // Params: numThreads, numObjects, objectSize, pollForUpdates, dropCaches
            run( Params{ numThreads, 100000, 256 * 1024, false, false } );
        }
    }

    // Test filesystem cache exhaustion.
    void testFilesystemCache()
    {
        const unsigned int MILLION = 1000000;
        for (unsigned int numObjects = 5 * MILLION; numObjects <= 100 * MILLION; numObjects += 5 * MILLION)
        {
            // Params: numThreads, numObjects, objectSize, pollForUpdates, dropCaches
            run( Params{12, numObjects, 4 * 1024, false, false} );
        }
    }
};

int main()
{
    ObjectStorePerfTest("object size.csv", false).testObjectSize();
    ObjectStorePerfTest("threading with 4K objects.csv", false).testThreads4K();
    ObjectStorePerfTest("threading with 12K objects.csv", false).testThreads12K();
    ObjectStorePerfTest("threading with 64K objects.csv", false ).testThreads64K();
    ObjectStorePerfTest("threading with 256K objects.csv", false ).testThreads256K();
    //ObjectStorePerfTest("filesystem cache.csv", false).testFilesystemCache();

    // Measure GDS
    // ObjectStorePerfTest("object size GDS.csv", true).testObjectSize();
    // ObjectStorePerfTest("threading with 4K objects GDS.csv", true).testThreads4K();
    // ObjectStorePerfTest("threading with 12K objects GDS.csv", true).testThreads12K();
    // ObjectStorePerfTest("filesystem cache.csv GDS", true).testFilesystemCache();

    return 0;
}
