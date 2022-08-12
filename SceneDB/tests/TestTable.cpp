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

#include <OptiXToolkit/SceneDB/Table.h>

#include <gtest/gtest.h>

#include <chrono>

using namespace sceneDB;

class TestTable : public testing::Test
{
  public:
    using Key = uint64_t;
    using Record = uint64_t;
    std::shared_ptr<Table<Key, Record>> m_table;

    void SetUp() { m_table = Table<Key, Record>::createInstance( "_tableDir", "_table" ); }

    void Destroy() { m_table.reset(); }
};

TEST_F(TestTable, TestCreateDestroy)
{
    auto writer = m_table->getWriter();
    EXPECT_TRUE( m_table->exists() );
    writer.reset();
    m_table->destroy();
}

TEST_F(TestTable, TestInsert)
{
    auto writer = m_table->getWriter();
    writer->insert( 1, 2 );

    writer.reset();
    m_table->destroy();
}

TEST_F(TestTable, TestWriteAndRead)
{
    auto writer = m_table->getWriter();
    writer->insert( 1, 2 );
    writer->insert( 3, 4 );
    writer.reset();

    auto reader = m_table->getReader();

    const Record* record1 = reader->find( 1 );
    EXPECT_NE( nullptr, record1 );
    EXPECT_EQ( 2, *record1 );

    const Record* record2 = reader->find( 3 );
    EXPECT_NE( nullptr, record2 );
    EXPECT_EQ( 4, *record2 );

    reader.reset();
    m_table->destroy();
}

TEST_F(TestTable, TestRemove)
{
    auto writer = m_table->getWriter();
    
    writer->insert( 1, 2 );
    writer->insert( 3, 4 );
    writer->remove( 1 );
    writer->insert( 1, 6 );
    writer->remove( 3 );
    writer.reset();

    auto reader = m_table->getReader();

    const Record* record1 = reader->find( 1 );
    EXPECT_NE( nullptr, record1);
    EXPECT_EQ( 6, *record1 );

    const Record* record2 = reader->find( 3 );
    EXPECT_EQ( nullptr, record2 );

    reader.reset();
    m_table->destroy();
}
