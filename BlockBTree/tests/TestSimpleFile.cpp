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

#include <OptiXToolkit/BlockBTree/SimpleFile.h>

#include <gtest/gtest.h>

using namespace sceneDB;

class TestSimpleFile : public testing::Test
{
};

TEST_F(TestSimpleFile, TestCreateDestroy)
{
    {
        SimpleFile file( "test.dat", true );
        EXPECT_TRUE( file.isWriteable() );
    }
    std::filesystem::remove( "test.dat" );
}

TEST_F(TestSimpleFile, TestWriteCloseRead)
{
    const char data[] = "Hello, world!";
    {
        SimpleFile file( "test.dat", true );
        file.write( data, sizeof( data ), 0 );
    }
    {
        SimpleFile file( "test.dat", false );
        char buffer[sizeof( data )];
        file.read( buffer, sizeof( buffer ), 0 );
        EXPECT_STREQ( data, buffer );
    }
    std::filesystem::remove( "test.dat" );
}

TEST_F(TestSimpleFile, TestWriteNoCloseRead)
{
    const char data[] = "Hello, world!";
    {
        SimpleFile file( "test.dat", true );
        file.write( data, sizeof( data ), 0 );

        char buffer[sizeof( data )];
        file.read( buffer, sizeof( buffer ), 0 );
        EXPECT_STREQ( data, buffer );
    }
    std::filesystem::remove( "test.dat" );
}
