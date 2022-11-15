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

#include <OptiXToolkit/BlockBTree/Table.h>
#include <OptiXToolkit/BlockBTree/TablePrinter.h>

#include <gtest/gtest.h>

#include <fstream>
#include <algorithm>
#include <random>
#include <numeric>

using namespace sceneDB;
constexpr size_t k_branchingFactor = 8;
constexpr size_t k_blockSize = 4096;
constexpr size_t k_blockAlignment = 8;

template <size_t bytes>
struct Record
{
    Record() = default;

    Record( const void* data )
    {
        memcpy( m_data, data, bytes );
    }

    Record& operator=( const Record& o )
    {
        memcpy( m_data, o.m_data, bytes );
        return *this;
    }

    char m_data[ bytes ];
};

template <size_t bytes>
bool operator==( const Record<bytes>& a, const Record<bytes>& b)
{
    return memcmp( a.m_data, b.m_data, bytes ) == 0;
}

using RecordT = Record<32>;
using KeyT = size_t;
using TableT = Table</*Key=*/KeyT, /*Record=*/RecordT, /*B=*/k_branchingFactor, /*BlockSize=*/k_blockSize, /*BlockAlignment=*/k_blockAlignment>;

class TestTable : public testing::Test
{
};

TEST_F(TestTable, TestCreate)
{
    TableT table( "test_table", "TestTable" );

    if( std::filesystem::exists( table.getDataFile() ) )
        std::filesystem::remove( table.getDataFile() );
    if( std::filesystem::exists( table.getSnapshotFile() ) )
        std::filesystem::remove( table.getSnapshotFile() );

    table.init( /*request_write=*/true );

    EXPECT_TRUE( std::filesystem::exists( table.getDataFile() ) );
    EXPECT_TRUE( std::filesystem::exists( table.getSnapshotFile() ) );

    table.destroy();

    EXPECT_FALSE( std::filesystem::exists( table.getDataFile() ) );
    EXPECT_FALSE( std::filesystem::exists( table.getSnapshotFile() ) );
}

TEST_F(TestTable, TestInsert)
{
    TableT table( "test_table", "TestTable" );
    table.init( /*request_write=*/true );
    auto writer( table.getWriter() );

    writer->Insert( 1, RecordT( std::string( std::to_string( 1 ) + " this is a long string. 32 bytes." ).c_str() ) );

    table.destroy();
}

TEST_F(TestTable, TestSnapshot)
{
    TableT table( "test_table", "TestTable" );

    if( std::filesystem::exists( table.getDataFile() ) )
        std::filesystem::remove( table.getDataFile() );
    if( std::filesystem::exists( table.getSnapshotFile() ) )
        std::filesystem::remove( table.getSnapshotFile() );

    table.init( /*request_write=*/true);
    auto writer(table.getWriter());

    std::vector<size_t> vec(1024*4);
    std::iota( vec.begin(), vec.end(), 0);

    auto rng = std::default_random_engine{};
    std::shuffle( vec.begin(), vec.end(), rng);

    for( size_t i = 0; i < 1024*4; ++i)
    {
        size_t j = vec[i];
        writer->Insert( 2*j, RecordT( std::string( std::to_string( 3 * j ) + " this is a long string. 32 bytes." ).c_str() ) );
    }
    auto snap = writer->TakeSnaphot();
    auto reader = table.getReader( snap );

    RecordT record;

    for (size_t i = 0; i < 1024*8; ++i)
    {
        if( i & 1 )
            EXPECT_FALSE( reader->Query( i, record ) );
        else
        {
            EXPECT_TRUE( reader->Query( i, record ) );
            EXPECT_EQ( RecordT( std::string( std::to_string( (i / 2) * 3 ) + " this is a long string. 32 bytes." ).c_str() ), record );
        }
    }
}

TEST_F(TestTable, TestReopenSnapshot)
{
    TableT table( "test_table", "TestTable" );

    table.init( /*request_write=*/false );
    std::shared_ptr< const Snapshot > snapshot;

    EXPECT_TRUE( table.findSnapshot( 0, snapshot ) );
    EXPECT_EQ( 0, snapshot->m_snapshot_id );
    EXPECT_FALSE( table.findSnapshot( 1, snapshot ) );

    snapshot = table.getLatestSnapshot();

    EXPECT_EQ( 0, snapshot->m_snapshot_id );
}

TEST_F(TestTable, TestReopenSnapshotAndRead)
{
    TableT table("test_table", "TestTable");

    table.init( /*request_write=*/false);
    std::shared_ptr< const Snapshot > snapshot;

    snapshot = table.getLatestSnapshot();
    EXPECT_EQ(0, snapshot->m_snapshot_id);

    auto reader = table.getReader( snapshot );

    RecordT record;

    for( size_t i = 0; i < 1024*8; ++i)
    {
        if( i & 1 )
            EXPECT_FALSE( reader->Query( i, record ) );
        else
        {
            EXPECT_TRUE( reader->Query( i, record ) );
            EXPECT_EQ( RecordT( std::string( std::to_string( (i / 2) * 3 ) + " this is a long string. 32 bytes." ).c_str() ), record );
        }
    }
}

TEST_F(TestTable, TestReopenSnapshotAndWrite)
{
    TableT table("test_table", "TestTable");

    table.init( /*request_write=*/true);
    auto writer( table.getWriter() );
    auto reader_old = table.getReader( table.getLatestSnapshot() );

    for( size_t i = 1024*4; i < 1024*8; ++i)
        writer->Insert( 2*i, RecordT( std::string( std::to_string( 3 * i ) + " this is a long string. 32 bytes." ).c_str() ) );

    auto snap = writer->TakeSnaphot();
    EXPECT_EQ( 1, snap->m_snapshot_id );
    auto reader_new = table.getReader( snap );

    RecordT record;

    for( size_t i = 0; i < 1024*16; ++i)
    {
        if( i & 1 )
            EXPECT_FALSE( reader_new->Query( i, record ) );
        else
        {
            EXPECT_TRUE( reader_new->Query( i, record ) );
            EXPECT_EQ( RecordT( std::string( std::to_string( (i / 2) * 3 ) + " this is a long string. 32 bytes." ).c_str() ), record );
            if( i < 1024*8 )
            {
                EXPECT_TRUE( reader_old->Query( i, record ) );
                EXPECT_EQ( RecordT( std::string( std::to_string( (i / 2) * 3 ) + " this is a long string. 32 bytes." ).c_str() ), record );
            }
            else
                EXPECT_FALSE( reader_old->Query( i, record ) );
        }
    }

    table.destroy();
}