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
#include <OptiXToolkit/Util/Stopwatch.h>

#include <gtest/gtest.h>

#include <fstream>
#include <algorithm>
#include <random>
#include <numeric>

using namespace sceneDB;
constexpr size_t k_branchingFactor = 64;
constexpr size_t k_blockSize = 512*1024;
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

constexpr size_t RecordSize = 32;
using RecordT = Record<RecordSize>;
using KeyT = size_t;
using TableT = Table</*Key=*/KeyT, /*Record=*/RecordT, /*B=*/k_branchingFactor, /*BlockSize=*/k_blockSize, /*BlockAlignment=*/k_blockAlignment>;

class TestTablePerf : public testing::Test
{
};

TEST_F(TestTablePerf, TestInsertion)
{
    TableT table( "test_table", "TestTable" );

    if( std::filesystem::exists( table.getDataFile() ) )
        std::filesystem::remove( table.getDataFile() );
    if( std::filesystem::exists( table.getSnapshotFile() ) )
        std::filesystem::remove( table.getSnapshotFile() );

    table.init( /*request_write=*/true);
    auto writer(table.getWriter());

    printf("Branching Factor: %zu.\n", k_branchingFactor);
    printf("Node size: %zuB.\n", sizeof(TableT::Node));
    printf("Record size: %zuB.\n", sizeof(RecordT));
    printf("Block Size: %zuB.\n", k_blockSize);
    printf("Can fit %g records / %g nodes per block.\n", double(k_blockSize) / double(sizeof(RecordT)), double(k_blockSize) / double(sizeof(TableT::Node)));

    const size_t count = 16 * 1024 * 1024;
    const size_t total_count = 4 * count;
    const bool   randomize = true;

    std::vector<size_t> vec( total_count );
    std::iota( vec.begin(), vec.end(), 0 );

    std::vector<RecordT> records( total_count );
    //for( size_t i = 0; i < total_count; ++i )
    //    records[ i ] = RecordT( std::string( "Number: " + std::to_string( i ) + ":: this is a long string. 32 bytes.").c_str());

    if( randomize )
    {
        auto rng = std::default_random_engine{};
        std::shuffle( vec.begin(), vec.end(), rng );
    }

    otk::Stopwatch time;

    for( size_t i = 0; i < count; ++i)
    {
        size_t j = vec[i];
        writer->Insert( j, records[ j ] );
    }

    auto snap   = writer->TakeSnaphot();

    printf("Took %g seconds to write %zu %zuB elements (%gMB total).\n", time.elapsed(), count, RecordSize, double(count * RecordSize) / double(1024 * 1024));
    printf("Inserted @ %g elt / s.\n", double(count) / time.elapsed());

    const size_t filesize = std::filesystem::file_size(table.getDataFile());
    printf("The datafile is %gMB in size. Storing %gB / elt (%g%% extra).\n",
        double(filesize) / (1024. * 1024.), double(filesize) / double(count), double(filesize*100) / double(count*RecordSize) - 100.);

    auto reader = table.getReader(snap);

    RecordT record;

    for (size_t i = 0; i < count; ++i)
    {
        size_t j = vec[i];
        EXPECT_TRUE(reader->Query(j, record));
        EXPECT_EQ(records[j], record);
    }

    table.close();
}