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

#include <OptiXToolkit/BlockBTree/TablePrinter.h>

#include <gtest/gtest.h>

#include <fstream>

using namespace sceneDB;
constexpr size_t k_branchingFactor = 8;
constexpr size_t k_blockSize = 1024;
constexpr size_t k_blockAlignment = 8;
using RecordT = size_t;
using KeyT = size_t;
using TableT = Table</*Key=*/KeyT, /*Record=*/RecordT, /*B=*/k_branchingFactor, /*BlockSize=*/k_blockSize, /*BlockAlignment=*/k_blockAlignment>;

class TestTablePrinter : public testing::Test
{
};

TEST_F(TestTablePrinter, Test)
{
    TableT table( "test_table", "TestTable" );

    if( std::filesystem::exists( table.getDataFile() ) )
        std::filesystem::remove( table.getDataFile() );
    if( std::filesystem::exists( table.getSnapshotFile() ) )
        std::filesystem::remove( table.getSnapshotFile() );

    table.init( /*request_write=*/true);
    auto writer(table.getWriter());

    for( size_t i = 0; i < 256; ++i)
        writer->Insert( 2*i, 3*i );

    auto snap = writer->TakeSnaphot();
    auto printer = table.getPrinter( snap );

    std::ofstream out( "table.dot" );
    printer->Print( out );
    out.close();
    std::cout << "Wrote table.dot" << std::endl;
}
