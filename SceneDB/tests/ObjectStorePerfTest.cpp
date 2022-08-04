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

#include <numeric>
#include <cstdio>

using namespace sceneDB;

class ObjectStorePerfTest
{
  public:
    ObjectStorePerfTest( const char* statsFilename )
        : m_statsFile( fopen( statsFilename, "w" ) )
    {
        OTK_ASSERT_MSG( m_statsFile != nullptr, "Error opening stats file" );
        fprintf( m_statsFile, "# threads\tobject size\twrite throughput (MB/s)\tread throughput (MB/s)\tmetadata throughput (MB/s)\n" );
        printf( "Writing \"%s\" ", statsFilename );
    }

    ~ObjectStorePerfTest()
    {
        fclose( m_statsFile );
        printf(" done\n");
    }

  private:
    const unsigned int m_maxThreads = std::thread::hardware_concurrency();
    FILE*              m_statsFile;

    struct Params
    {
        unsigned int numThreads;
        size_t       numObjects;
        size_t       objectSize;
    };

    void run( const Params& params )
    {
        // Create ObjectStore
        std::shared_ptr<ObjectStore> store( ObjectStore::getInstance( ObjectStore::Options{"_testObjectStorePerf"} ) );

        // Create vector of object sizes.
        std::vector<size_t> objectSizes( params.numObjects, params.objectSize );

        // Write the objects.  The writer is scoped to ensure that it's closed properly.
        ObjectWriters  writers( store, objectSizes, params.numThreads );
        otk::Stopwatch writeTimer;
        writers.write();
        double writeTime   = writeTimer.elapsed();

        // Report writer stats.
        float  writeSizeMB = std::accumulate( objectSizes.begin(), objectSizes.end(), 0UL ) / ( 1024 * 1024.f );
        
        // Construct ObjectReaders, which reads the object metadata file.
        otk::Stopwatch metadataTimer;
        ObjectReaders  readers( store, objectSizes, /*validateData=*/false, params.numThreads );
        double metadataTime   = metadataTimer.elapsed();

        // Read the objects, validating their contents.
        otk::Stopwatch dataTimer;
        readers.read();
        double dataTime = dataTimer.elapsed();

        // Print stats.
        float  objectsSizeMB = std::accumulate( objectSizes.begin(), objectSizes.end(), 0UL ) / ( 1024 * 1024.f );
        float  metadataSizeMB = objectSizes.size() * sizeof( sceneDB::ObjectMetadata ) / ( 1024 * 1024.f );
        fprintf( m_statsFile, "%i\t%zu\t%g\t%g\t%g\n", params.numThreads, objectSizes[0], objectsSizeMB / writeTime,
                 objectsSizeMB / dataTime, metadataSizeMB / metadataTime );
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
        for( size_t size = 64; size <= 1024; size += 64 )
        {
            // Params: numThreads, numObjects, objectSize
            run( Params{numThreads, 10000, size} );
        }
        for( size_t size = 512; size <= 16 * 1024; size += 512 )
        {
            // Params: numThreads, numObjects, objectSize
            run( Params{numThreads, 10000, size} );
        }
    }

    // Test 4K object throughput vs. thread count.
    void testThreads4K()
    {
        for( unsigned int numThreads = 1; numThreads <= static_cast<unsigned int>( 1.5f * m_maxThreads ); ++numThreads )
        {
            // Params: numThreads, numObjects, objectSize
            run( Params{numThreads, 10000, 4 * 1024} );
        }
    }

    // Test 12K object throughput vs. thread count.
    void testThreads12K()
    {
        for( unsigned int numThreads = 1; numThreads <= static_cast<unsigned int>( 1.5f * m_maxThreads ); ++numThreads )
        {
            // Params: numThreads, numObjects, objectSize
            run( Params{numThreads, 10000, 12 * 1024} );
        }
    }
};

int main()
{
    ObjectStorePerfTest("object size.csv").testObjectSize();
    ObjectStorePerfTest("threading with 4K objects.csv").testThreads4K();
    ObjectStorePerfTest("threading with 12K objects.csv").testThreads12K();
    return 0;
}
