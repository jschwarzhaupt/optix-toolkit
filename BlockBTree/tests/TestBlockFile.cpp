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

#include <OptiXToolkit/BlockBTree/BlockFile.h>
#include <filesystem>

#include <gtest/gtest.h>

using namespace sceneDB;

class TestBlockFile : public testing::Test
{  
};

constexpr char* k_filename = "test_blockfile.dat";

TEST_F(TestBlockFile, TestHeader)
{
	BlockFile::Header header;
    EXPECT_EQ( BlockFile::Header::k_version, header.m_version );
    EXPECT_EQ( 0, header.m_blockSize );
    EXPECT_EQ( 0, header.m_blockAlignment );
    EXPECT_EQ( 0, header.m_nextBlock );
    EXPECT_EQ( 0, header.m_freeListSize );
}

TEST_F(TestBlockFile, TestCreateDestroyWriteable)
{
    if( std::filesystem::exists( k_filename ) )
        std::filesystem::remove( k_filename );
    std::shared_ptr< BlockFile::Header > header = std::make_shared< BlockFile::Header >();
    BlockFile file( k_filename, header, 512, 8, true );
    EXPECT_EQ( 512, header->m_blockSize );
    EXPECT_EQ( 8, header->m_blockAlignment );
    EXPECT_EQ( 0, header->m_nextBlock );
    EXPECT_EQ( 0, header->m_freeListSize );
    EXPECT_TRUE( file.isWriteable() );
    EXPECT_FALSE( file.exists( 0 ) );
    EXPECT_TRUE( std::filesystem::exists( k_filename ) );
    file.destroy();
    EXPECT_FALSE( std::filesystem::exists( k_filename ) );
}

TEST_F(TestBlockFile, TestWrite)
{
    std::shared_ptr< BlockFile::Header > header = std::make_shared< BlockFile::Header >();
    BlockFile file( k_filename, header, 512, 8, true );
    auto block_0 = file.checkOutNewBlock();
    EXPECT_EQ( 0, block_0->index );
    EXPECT_EQ( 512, block_0->size );
    EXPECT_TRUE( block_0->is_valid() );
    strcpy( reinterpret_cast<char*>( block_0->get_data() ), "Hello world from block 0!" );
    auto block_1 = file.checkOutNewBlock();
    EXPECT_EQ( 1, block_1->index );
    EXPECT_TRUE( block_1->is_valid() );
    strcpy( reinterpret_cast<char*>( block_1->get_data() ), "Hello world from block 1!" );

    EXPECT_THROW( file.checkOutForRead( 0 ), otk::Exception );
}

TEST_F(TestBlockFile, TestCreateReadOnly)
{
    std::shared_ptr< BlockFile::Header > header = std::make_shared< BlockFile::Header >();
    BlockFile file( k_filename, header, 512, 8, false );
    EXPECT_EQ( 512, header->m_blockSize );
    EXPECT_EQ( 8, header->m_blockAlignment );
    EXPECT_EQ( 2, header->m_nextBlock );
    EXPECT_EQ( 0, header->m_freeListSize );
    EXPECT_FALSE( file.isWriteable() );

    EXPECT_THROW( file.checkOutForReadWrite( 0 ), otk::Exception );
}

TEST_F(TestBlockFile, TestRead)
{
    std::shared_ptr< BlockFile::Header > header = std::make_shared< BlockFile::Header >();
    BlockFile file(k_filename, header, 512, 8, false);
    EXPECT_TRUE(file.exists(0));
    EXPECT_TRUE(file.exists(1));
    auto block_0 = file.checkOutForRead(0);
    EXPECT_EQ(0, block_0->index);
    EXPECT_EQ(512, block_0->size);
    EXPECT_TRUE(block_0->is_valid());
    EXPECT_STREQ(reinterpret_cast<const char*>(block_0->get_data()), "Hello world from block 0!");
    auto block_1 = file.checkOutForRead(1);
    EXPECT_EQ(1, block_1->index);
    EXPECT_TRUE(block_1->is_valid());
    EXPECT_STREQ(reinterpret_cast<const char*>(block_1->get_data()), "Hello world from block 1!");

    EXPECT_THROW( file.checkOutNewBlock(), otk::Exception );
}

TEST_F(TestBlockFile, TestBounds )
{
    std::shared_ptr< BlockFile::Header > header = std::make_shared< BlockFile::Header >();
    BlockFile file( k_filename, header, 512, 8, false );
    EXPECT_TRUE( file.exists(1) );
    EXPECT_FALSE( file.exists(2) );
    EXPECT_THROW( file.checkOutForRead( 2 ), otk::Exception );
    EXPECT_THROW( file.checkOutForReadWrite( 2 ), otk::Exception );
}

TEST_F(TestBlockFile, TestAppendToExisting)
{
    std::shared_ptr< BlockFile::Header > header = std::make_shared< BlockFile::Header >();
    BlockFile file( k_filename, header, 512, 8, true );
    EXPECT_TRUE( file.exists(0) );
    EXPECT_TRUE( file.exists(1) );
    auto block_2 = file.checkOutNewBlock();
    EXPECT_EQ( 2, block_2->index );
    EXPECT_EQ( 512, block_2->size );
    strcpy( reinterpret_cast<char*>( block_2->get_data() ), "Here is block number 2!" );
}

TEST_F(TestBlockFile, TestOverwrite)
{
    std::shared_ptr< BlockFile::Header > header = std::make_shared< BlockFile::Header >();
    BlockFile file( k_filename, header, 512, 8, true );
    EXPECT_TRUE( file.exists(2) );
    auto block_2 = file.checkOutForReadWrite( 2 );
    EXPECT_EQ( 2, block_2->index );
    EXPECT_STREQ( reinterpret_cast<const char*>( block_2->get_data() ), "Here is block number 2!" );
    strcpy( reinterpret_cast<char*>( block_2->get_data() ), "This block has been overwritten. It still is block number 2." );
    file.flush( true );
    file.close();

    std::shared_ptr< BlockFile::Header > header_read = std::make_shared< BlockFile::Header >();
    BlockFile file_read( k_filename, header_read, 512, 8, false );
    EXPECT_TRUE( file_read.exists(2) );
    auto block_2_read = file_read.checkOutForRead( 2 );
    EXPECT_EQ( 2, block_2_read->index );
    EXPECT_STREQ( reinterpret_cast<const char*>( block_2_read->get_data() ), "This block has been overwritten. It still is block number 2." );
}

TEST_F(TestBlockFile, TestUnload)
{
    std::shared_ptr< BlockFile::Header > header = std::make_shared< BlockFile::Header >();
    BlockFile file( k_filename, header, 512, 8, true );
    EXPECT_TRUE( file.exists(2) );
    auto block_2 = file.checkOutForReadWrite( 2 );
    EXPECT_TRUE( block_2->is_valid() );

    file.unloadBlock( 2 );
    EXPECT_FALSE( block_2->is_valid() );

    auto block_2_read = file.checkOutForRead( 2 );
    EXPECT_TRUE( block_2_read->is_valid() );
    EXPECT_STREQ( reinterpret_cast<const char*>( block_2_read->get_data() ), "This block has been overwritten. It still is block number 2." );
}

TEST_F(TestBlockFile, TestFree)
{
    std::shared_ptr< BlockFile::Header > header = std::make_shared< BlockFile::Header >();
    BlockFile file( k_filename, header, 512, 8, true );
    EXPECT_TRUE( file.exists(1) );
    auto block_1 = file.checkOutForReadWrite( 1 );
    EXPECT_TRUE( block_1->is_valid() );

    file.freeBlock( 1 );
    EXPECT_FALSE( block_1->is_valid() );
    block_1.reset();

    EXPECT_THROW( file.checkOutForReadWrite( 1 ), otk::Exception );
    EXPECT_THROW( file.checkOutForRead( 1 ), otk::Exception );
    block_1 = file.checkOutNewBlock();
    EXPECT_EQ( 1, block_1->index );
    EXPECT_TRUE( block_1->is_valid() );

    auto block_3 = file.checkOutNewBlock();
    EXPECT_EQ( 3, block_3->index );

    file.freeBlock( 3 );
    EXPECT_FALSE( block_3->is_valid() );
}

TEST_F(TestBlockFile, TestDestroy)
{
    std::shared_ptr< BlockFile::Header > header = std::make_shared< BlockFile::Header >();
    BlockFile file( k_filename, header, 512, 8, true );
    EXPECT_EQ( 4, header->m_nextBlock );
    EXPECT_EQ( 1, header->m_freeListSize );
    EXPECT_TRUE( file.isWriteable() );
    EXPECT_FALSE( file.exists( 3 ) );
    EXPECT_TRUE( std::filesystem::exists( k_filename ) );
    file.destroy();
    EXPECT_FALSE( std::filesystem::exists( k_filename ) );
}