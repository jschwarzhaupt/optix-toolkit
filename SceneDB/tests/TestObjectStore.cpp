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

#include <OptiXToolkit/SceneDB/ObjectStoreWriter.h>
#include <OptiXToolkit/SceneDB/ObjectStoreReader.h>

#include <gtest/gtest.h>

using namespace otk;

class TestObjectStore : public testing::Test
{
};

TEST_F(TestObjectStore, TestCtors)
{
    ObjectStoreWriter writer("_store");
    ObjectStoreReader reader("_store");
}

TEST_F(TestObjectStore, TestInsert)
{
    ObjectStoreWriter writer("_store");
    const char* str = "Hello, world!";
    writer.insert( 1, str, strlen( str ) );
}

TEST_F(TestObjectStore, TestWriteAndRead)
{
    const char* store = "_store_testWriteAndRead";
    ObjectStoreWriter writer(store);
    const char* str1 = "Hello, world!";
    const char* str2 = "Goodbye, cruel world.";
    writer.insert( 1, str1, strlen( str1 ) );
    writer.insert( 2, str2, strlen( str2 ) );
    writer.synchronize();

    ObjectStoreReader reader(store);

    std::vector<char> buf1(strlen(str1)+1);
    size_t size1;
    EXPECT_TRUE( reader.find( 1, buf1.data(), buf1.size(), size1 ) );
    buf1[strlen( str1 )] = '\0';
    EXPECT_STREQ( str1, buf1.data() );

    std::vector<char> buf2;
    EXPECT_TRUE( reader.find( 2, buf2 ) );
    EXPECT_EQ( std::string( str2 ), std::string( buf2.data(), buf2.size() ) );
}
