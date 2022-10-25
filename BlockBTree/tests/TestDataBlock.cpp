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

#include <OptiXToolkit/BlockBTree/DataBlock.h>

#include <gtest/gtest.h>

using namespace sceneDB;

class TestDataBlock : public testing::Test
{
};

TEST_F(TestDataBlock, TestCreateDestroy)
{
    DataBlock block( 128, 4, 0 );
    EXPECT_EQ( 128, block.size );
    EXPECT_EQ( 0, block.index );
    EXPECT_NE( nullptr, block.get_data() );
    EXPECT_EQ( 0, (unsigned long long)(block.get_data()) & 3 );
    EXPECT_FALSE( block.is_valid() );
}

TEST_F(TestDataBlock, TestCopy)
{
    DataBlock block1( 128, 4, 0 );
    strcpy( reinterpret_cast<char*>( block1.get_data() ), "Hello, world!" );

    DataBlock block2( 128, 4, 0 );
    block2 = block1;
    EXPECT_STREQ( reinterpret_cast<const char*>( block1.get_data() ), reinterpret_cast<const char*>( block2.get_data() ) );
    EXPECT_TRUE( block2.is_valid() );
    EXPECT_FALSE( block1.is_valid() );
}

TEST_F(TestDataBlock, TestAlignment)
{
    size_t alignment[8] = { 1, 2, 4, 8, 16, 32, 64, 128 };
    for( size_t i = 0; i < 8; ++i )
    {
        DataBlock block( 512, alignment[ i ], i );
        EXPECT_EQ( 512, block.size );
        EXPECT_EQ( i, block.index );
        EXPECT_NE( nullptr, block.get_data() );
        EXPECT_EQ( 0, (unsigned long long)(block.get_data()) & ( alignment[ i ] - 1 ) );
    }
}
