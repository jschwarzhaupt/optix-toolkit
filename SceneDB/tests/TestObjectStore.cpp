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
#include <OptiXToolkit/Util/Exception.h>

#include <gtest/gtest.h>

#include <cuda_runtime.h>

#include <chrono>
#include <thread>

using namespace sceneDB;

class TestObjectStore : public testing::Test
{
public:
    std::shared_ptr<ObjectStore> m_store;

    void SetUp() { m_store = ObjectStore::createInstance( ObjectStore::Options{"_store"} ); }
};

TEST_F(TestObjectStore, TestCreateDestroy)
{
    auto writer = m_store->getWriter();
    EXPECT_TRUE( m_store->exists() );
    writer.reset();
    m_store->destroy();
}

TEST_F(TestObjectStore, TestInsert)
{
    auto writer = m_store->getWriter();
    const char* str = "Hello, world!";
    writer->insert( 1, str, strlen( str ) );

    writer.reset();
    m_store->destroy();
}

TEST_F(TestObjectStore, TestWriteAndRead)
{
    auto writer = m_store->getWriter();
    const char* str1 = "Hello, world!";
    const char* str2 = "Goodbye, cruel world.";
    writer->insert( 1, str1, strlen( str1 ) );
    writer->insert( 2, str2, strlen( str2 ) );
    writer.reset();

    ObjectStoreReader::Options options;
    options.pollForUpdates = false;
    auto reader = m_store->getReader( options );

    std::vector<char> buf1(strlen(str1)+1);
    size_t size1;
    EXPECT_TRUE( reader->find( 1, buf1.data(), buf1.size(), size1 ) );
    buf1[strlen( str1 )] = '\0';
    EXPECT_STREQ( str1, buf1.data() );

    void* buf2 = 0;
    size_t size2 = 0;
    EXPECT_TRUE( reader->find( 2, buf2, size2 ) );
    EXPECT_EQ( std::string( str2 ), std::string( static_cast<const char*>(buf2), size2 ) );

    reader.reset();
    m_store->destroy();
}

TEST_F(TestObjectStore, TestReadGpuDirectStorage)
{
    auto writer = m_store->getWriter();
    const char* str1 = "Hello, world!";
    const char* str2 = "Goodbye, cruel world.";
    writer->insert( 1, str1, strlen( str1 ) );
    writer->insert( 2, str2, strlen( str2 ) );
    writer.reset();

    ObjectStoreReader::Options options;
    options.pollForUpdates = false;
    options.useGds = true;
    auto reader = m_store->getReader( options );

    void* devptr1 = 0;
    size_t size1 = 0;
    const size_t str1Size = strlen( str1 );
    CUDA_CHECK( cudaMalloc( &devptr1, str1Size ) );
    EXPECT_TRUE( reader->find( 1, devptr1, str1Size, size1 ) );
    std::vector<char> buf1(size1);
    CUDA_CHECK( cudaMemcpy( reinterpret_cast<void*>(buf1.data()), devptr1, size1, cudaMemcpyDeviceToHost ) );
    buf1[size1] = '\0';
    EXPECT_STREQ( str1, buf1.data() ); 

    void* devptr2 = 0;
    size_t size2 = 0;
    const size_t str2Size = strlen( str2 );
    EXPECT_TRUE( reader->find( 2, devptr2, size2 ) );
    std::vector<char> buf2(size2);
    CUDA_CHECK( cudaMemcpy( reinterpret_cast<void*>(buf2.data()), devptr2, size2, cudaMemcpyDeviceToHost ) );
    buf2[size2] = '\0';
    EXPECT_STREQ( str2, buf2.data() );

    reader.reset();
    m_store->destroy();
}

TEST_F(TestObjectStore, TestRemove)
{
    auto writer = m_store->getWriter();
    const char* str1 = "Hello, world!";
    const char* str2 = "Goodbye, cruel world.";
    const char* str3 = "What a wonderful world.";
    writer->insert( 1, str1, strlen( str1 ) );
    writer->insert( 2, str2, strlen( str2 ) );
    writer->remove( 1 );
    writer->insert( 1, str3, strlen( str3 ) );
    writer->remove( 2 );
    writer.reset();

    ObjectStoreReader::Options options;
    options.pollForUpdates = false;
    auto reader = m_store->getReader( options );

    std::vector<char> buf1(strlen(str3)+1);
    size_t size1;
    EXPECT_TRUE( reader->find( 1, buf1.data(), buf1.size(), size1 ) );
    buf1[strlen( str3 )] = '\0';
    EXPECT_STREQ( str3, buf1.data() );

    void* buf2 = 0;
    size_t size2 = 0;
    EXPECT_FALSE( reader->find( 2, buf2, size2 ) );

    reader.reset();
    m_store->destroy();
}

TEST_F(TestObjectStore, DISABLED_TestReaderPolling)
{
    using namespace std::chrono_literals;

    // Create writer.
    auto writer = m_store->getWriter();

    // Create reader with polling enabled.
    ObjectStoreReader::Options options;
    options.pollForUpdates = true;
    auto reader = m_store->getReader( options );

    // Write first object.
    const char* str1 = "Hello, world!";
    writer->insert( 1, str1, strlen( str1 ) );
    writer->flush();

    // Avoid racing with the reader while it asynchronously reads metadata.
    std::this_thread::sleep_for( 100ms );

    // Read first object.
    std::vector<char> buf1(strlen(str1)+1);
    size_t size1;
    EXPECT_TRUE( reader->find( 1, buf1.data(), buf1.size(), size1 ) );
    buf1[strlen( str1 )] = '\0';
    EXPECT_STREQ( str1, buf1.data() );

    // Write second object.
    const char* str2 = "Goodbye, cruel world.";
    writer->insert( 2, str2, strlen( str2 ) );
    writer->flush();

    // Avoid racing with the reader while it asynchronously reads metadata.
    std::this_thread::sleep_for( 100ms );

    // Read second object.
    void* buf2 = 0;
    size_t size2 = 0;
    EXPECT_TRUE( reader->find( 2, buf2, size2 ) );
    EXPECT_EQ( std::string( str2 ), std::string( static_cast<const char*>(buf2), size2 ) );

    // Clean up.
    writer.reset();
    reader.reset();
    m_store->destroy();
}
