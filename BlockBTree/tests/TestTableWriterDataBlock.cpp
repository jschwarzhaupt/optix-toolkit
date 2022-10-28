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

#include <gtest/gtest.h>

using namespace sceneDB;
constexpr size_t k_branchingFactor = 4;
constexpr size_t k_blockSize = 4096;
constexpr size_t k_blockAlignment = 8;
constexpr size_t k_blockIndex = 9;
using RecordT = int;
using KeyT    = size_t;
using BlockT = TableWriterDataBlock</*Key=*/KeyT, /*Record=*/RecordT, /*B=*/k_branchingFactor, /*BlockSize=*/k_blockSize, /*BlockAlignment=*/k_blockAlignment>;
using NodeT  = BlockT::Node;

class TestTableWriterDataBlock : public testing::Test
{
public:
    Snapshot                     m_snapshot;
    std::shared_ptr< DataBlock > m_dataBlock;
    std::unique_ptr< BlockT >    m_block;

    void SetUp()
    { 
        m_dataBlock = std::make_shared< DataBlock >( k_blockSize, k_blockAlignment, k_blockIndex );
        m_dataBlock->set_valid( true );
        m_block.reset( new BlockT( m_dataBlock ) );
        m_block->init( m_snapshot );
    }

    void TearDown()
    {
        m_block.reset();
        m_dataBlock.reset();
    }
};

TEST_F(TestTableWriterDataBlock, TestRootData)
{
    BlockT::RootData rootData;
    EXPECT_EQ( 0, rootData.m_root_count );
    for( size_t i = 0; i < k_branchingFactor; ++i )
    {
        EXPECT_NO_THROW( rootData.add_root( i ) );
    }
    EXPECT_EQ( k_branchingFactor, rootData.m_root_count );
    EXPECT_THROW( rootData.add_root( k_branchingFactor ), otk::Exception );
    EXPECT_FALSE( rootData.is_root( k_branchingFactor ) );
    EXPECT_TRUE( rootData.is_root( 0 ) );
    rootData.delete_root( 0 );
    EXPECT_FALSE( rootData.is_root( 0 ) );
    EXPECT_EQ( k_branchingFactor-1, rootData.m_root_count );
    EXPECT_NO_THROW( rootData.add_root( k_branchingFactor ) );
    EXPECT_TRUE( rootData.is_root( k_branchingFactor ) );
}

TEST_F(TestTableWriterDataBlock, TestCreate)
{
    BlockT block( m_dataBlock );
    EXPECT_EQ( k_blockIndex, block.index() );
    EXPECT_FALSE( block.is_valid() );
}

TEST_F(TestTableWriterDataBlock, TestInit)
{
    EXPECT_TRUE( m_block->is_valid() );
}

TEST_F(TestTableWriterDataBlock, TestValidateEmpty)
{
    m_block->release();
    EXPECT_FALSE( m_block->is_valid() );

    std::shared_ptr< DataBlock > wrongDataBlock = std::make_shared< DataBlock >( k_blockSize / 2, k_blockAlignment, k_blockIndex );
    EXPECT_THROW( BlockT block2( wrongDataBlock ), otk::Exception );

    EXPECT_THROW( m_block->set_data( wrongDataBlock ), otk::Exception );
    EXPECT_NO_THROW( m_block->set_data( m_dataBlock ) );
    m_block->init_existing();
    EXPECT_TRUE( m_block->is_valid() );
    EXPECT_FALSE( m_block->is_full() );
    EXPECT_TRUE( m_block->is_root( 0 ) );
}

TEST_F(TestTableWriterDataBlock, TestAddNode)
{
    uint32_t offset = m_block->add_node();
    EXPECT_NE( 0, offset );
    EXPECT_EQ( sizeof(BlockT::Node), offset );
    EXPECT_NE( uint32_t(-1), offset );
    EXPECT_FALSE( m_block->is_root( offset ) );

    NodeT* node = nullptr;
    EXPECT_NO_THROW( node = m_block->node(offset) );
    EXPECT_NE( nullptr, node );
}

TEST_F(TestTableWriterDataBlock, TestNodeBoundsAndAlignmentCheck)
{
    uint32_t offset = m_block->add_node();
    EXPECT_NO_THROW( m_block->node( offset ) );
    EXPECT_THROW( m_block->node( offset + sizeof(BlockT::Node) ), otk::Exception );
    EXPECT_THROW( m_block->node( offset + 1 ), otk::Exception );
    EXPECT_THROW( m_block->node( offset - 1 ), otk::Exception );
}

TEST_F(TestTableWriterDataBlock, TestRemoveNode)
{
    size_t no_compact_free_bytes_before = m_block->no_compact_free_bytes();
    size_t total_free_bytes_before      = m_block->total_free_bytes();
    EXPECT_EQ( 0, m_block->compact_nodes_free_bytes() );

    uint32_t offset_1 = m_block->add_node();
    uint32_t offset_2 = m_block->add_node();

    EXPECT_EQ( 0, m_block->compact_nodes_free_bytes() );
    EXPECT_EQ( no_compact_free_bytes_before - 2 * sizeof(BlockT::Node), m_block->no_compact_free_bytes() );
    EXPECT_EQ( total_free_bytes_before - 2 * sizeof(BlockT::Node), m_block->total_free_bytes() );

    m_block->remove_node( offset_2 );

    EXPECT_EQ( 0, m_block->compact_nodes_free_bytes() );
    EXPECT_EQ( no_compact_free_bytes_before - 1 * sizeof(BlockT::Node), m_block->no_compact_free_bytes() );
    EXPECT_EQ( total_free_bytes_before - 1 * sizeof(BlockT::Node), m_block->total_free_bytes() );

    EXPECT_EQ( offset_2, m_block->add_node() );
    EXPECT_EQ( 0, m_block->compact_nodes_free_bytes() );
    EXPECT_EQ( no_compact_free_bytes_before - 2 * sizeof(BlockT::Node), m_block->no_compact_free_bytes() );
    EXPECT_EQ( total_free_bytes_before - 2 * sizeof(BlockT::Node), m_block->total_free_bytes() );

    m_block->remove_node( offset_1 );

    EXPECT_EQ( sizeof(BlockT::Node), m_block->compact_nodes_free_bytes() );
    EXPECT_EQ( no_compact_free_bytes_before - 2 * sizeof(BlockT::Node), m_block->no_compact_free_bytes() );
    EXPECT_EQ( total_free_bytes_before - 1 * sizeof(BlockT::Node), m_block->total_free_bytes() );
}

TEST_F(TestTableWriterDataBlock, TestAddRecord)
{
    RecordT record_0 = 1234;
    uint32_t offset = m_block->add_record( record_0 );
    EXPECT_NE( 0, offset );
    EXPECT_GT( k_blockSize, offset );
    EXPECT_THROW( m_block->node( offset ), otk::Exception );
    EXPECT_NO_THROW( m_block->record( offset ) );
    EXPECT_EQ( record_0, m_block->record( offset ) );
}

TEST_F(TestTableWriterDataBlock, TestRecordBoundsAndAlignmentCheck)
{
    RecordT record_0 = 1234;
    uint32_t offset = m_block->add_record( record_0 );
    EXPECT_NO_THROW( m_block->record( offset ) );
    EXPECT_THROW( m_block->record( offset + sizeof(RecordT) ), otk::Exception );
    EXPECT_THROW( m_block->record( offset - sizeof(RecordT) ), otk::Exception );
    EXPECT_THROW( m_block->record( offset + 1 ), otk::Exception );
    EXPECT_THROW( m_block->record( offset - 1 ), otk::Exception );
}

TEST_F(TestTableWriterDataBlock, TestRemoveRecord)
{
    size_t no_compact_free_bytes_before = m_block->no_compact_free_bytes();
    size_t total_free_bytes_before      = m_block->total_free_bytes();
    EXPECT_EQ( 0, m_block->compact_records_free_bytes() );

    RecordT record_0 = 1234;
    RecordT record_1 = 5678;
    uint32_t offset_0 = m_block->add_record( record_0 );
    uint32_t offset_1 = m_block->add_record( record_1 );

    EXPECT_EQ( 0, m_block->compact_records_free_bytes() );
    EXPECT_EQ( no_compact_free_bytes_before - 2 * sizeof(RecordT), m_block->no_compact_free_bytes() );
    EXPECT_EQ( total_free_bytes_before - 2 * sizeof(RecordT), m_block->total_free_bytes() );

    m_block->remove_record( offset_1 );

    EXPECT_EQ( 0, m_block->compact_records_free_bytes() );
    EXPECT_EQ( no_compact_free_bytes_before - 1 * sizeof(RecordT), m_block->no_compact_free_bytes() );
    EXPECT_EQ( total_free_bytes_before - 1 * sizeof(RecordT), m_block->total_free_bytes() );

    EXPECT_EQ( offset_1, m_block->add_record( record_1 ) );
    EXPECT_EQ( 0, m_block->compact_records_free_bytes() );
    EXPECT_EQ( no_compact_free_bytes_before - 2 * sizeof(RecordT), m_block->no_compact_free_bytes() );
    EXPECT_EQ( total_free_bytes_before - 2 * sizeof(RecordT), m_block->total_free_bytes() );

    m_block->remove_record( offset_0 );

    EXPECT_EQ( sizeof(RecordT), m_block->compact_records_free_bytes() );
    EXPECT_EQ( no_compact_free_bytes_before - 2 * sizeof(RecordT), m_block->no_compact_free_bytes() );
    EXPECT_EQ( total_free_bytes_before - 1 * sizeof(RecordT), m_block->total_free_bytes() );
}

TEST_F(TestTableWriterDataBlock, TestAddRemoveRoot)
{
    EXPECT_EQ( 1, m_block->root_count() );
    EXPECT_TRUE( m_block->is_root( 0 ) );

    uint32_t offset_1 = m_block->add_node();
    uint32_t offset_2 = m_block->add_node();
    uint32_t offset_3 = m_block->add_node();

    EXPECT_FALSE( m_block->is_root( offset_1 ) );

    m_block->add_root( offset_1 );
    m_block->add_root( offset_2 );

    EXPECT_TRUE( m_block->is_root( offset_1 ) );
    EXPECT_TRUE( m_block->is_root( offset_2 ) );
    EXPECT_FALSE( m_block->is_root( offset_3 ) );
    EXPECT_EQ( 3, m_block->root_count() );
    
    m_block->remove_root( offset_1 );
    EXPECT_FALSE( m_block->is_root( offset_1 ) );
    EXPECT_EQ( 2, m_block->root_count() );

    m_block->remove_root( offset_3 );
    EXPECT_EQ( 2, m_block->root_count() );
}

TEST_F(TestTableWriterDataBlock, TestCompactNodes)
{
    BlockT::Node parent_node;

    const size_t node_count = 3 * k_branchingFactor;
    uint32_t offsets[ node_count ];
    offsets[0] = 0;
    for( size_t i = 1; i < node_count; ++i )
    {
        offsets[ i ] = m_block->add_node();
        if( i % 3 == 0 )
            m_block->add_root( offsets[ i ] );
    }

    for( size_t i = 0; i < node_count; ++i )
    {
        BlockT::Link link( /*leaf=*/false, /*local-*/false, /*block_index=*/m_block->index(), /*local_address=*/offsets[i] );
        if( i % 3 == 0 )
        {
            parent_node.insert( { KeyT(i), link } );
        }
        BlockT::Node* node = m_block->node( offsets[ i ] );
        node->insert( { KeyT(i), link } );
    }

    size_t no_compact_free_bytes_before = m_block->no_compact_free_bytes();
    size_t total_free_bytes_before      = m_block->total_free_bytes();
    EXPECT_EQ( 0, m_block->compact_nodes_free_bytes() );

    for( size_t i = 0; i < node_count; ++i )
    {
        if( i % 3 != 0 )
        {
            m_block->remove_node( offsets[ i ] );
        }
    }

    EXPECT_EQ( (node_count - k_branchingFactor - 2) * sizeof(BlockT::Node), m_block->compact_nodes_free_bytes() );
    EXPECT_EQ( no_compact_free_bytes_before + 2 * sizeof(BlockT::Node), m_block->no_compact_free_bytes() );
    EXPECT_EQ( total_free_bytes_before + 2 * k_branchingFactor * sizeof(BlockT::Node), m_block->total_free_bytes() );

    m_block->compact_nodes( &parent_node );

    EXPECT_EQ( m_block->no_compact_free_bytes(), m_block->total_free_bytes() );
    EXPECT_EQ( 0, m_block->compact_nodes_free_bytes() );
    EXPECT_EQ( total_free_bytes_before + 2 * k_branchingFactor * sizeof(BlockT::Node), m_block->total_free_bytes() );

    for( size_t i = 0; i < k_branchingFactor; ++i )
    {
        BlockT::Pair_T& pair = parent_node.get_pair( i );

        EXPECT_TRUE( m_block->is_root( pair.second.get_local_address() ) );
        EXPECT_EQ( KeyT( 3 * i ), m_block->node( pair.second.get_local_address() )->get_pair( 0 ).first );
    }
}

TEST_F(TestTableWriterDataBlock, TestCompactRecords)
{
    BlockT::Node* node_0 = m_block->node( 0 );
    uint32_t offset_node_1 = m_block->add_node();
    BlockT::Node* node_1 = m_block->node( offset_node_1 );

    uint32_t record_offsets[ 4 * k_branchingFactor ];
    RecordT records[ 4 * k_branchingFactor ];
    KeyT keys[ 4 * k_branchingFactor ];

    for( uint32_t i = 0; i < 4 * k_branchingFactor; ++i )
    {
        records[ i ] = RecordT( 3 * i );
        keys[ i ]    = KeyT( 5 * i );
        record_offsets[ i ] = m_block->add_record( records[ i ] );
        if( i & 1 )
        {
            BlockT::Link link( true, true, m_block->index(), record_offsets[ i ] );
            if( i < 2 * k_branchingFactor )
                node_0->insert( { keys[ i ], link } );
            else
                node_1->insert( { keys[ i ], link } );
        }
    }

    size_t no_compact_free_bytes_before = m_block->no_compact_free_bytes();
    size_t total_free_bytes_before      = m_block->total_free_bytes();
    EXPECT_EQ( 0, m_block->compact_records_free_bytes() );

    for( uint32_t i = 0; i < 4 * k_branchingFactor; ++i )
    {
        if( !(i & 1) )
            m_block->remove_record( record_offsets[ i ] );
    }

    EXPECT_EQ( 2 * k_branchingFactor * sizeof(RecordT), m_block->compact_records_free_bytes() );
    EXPECT_EQ( no_compact_free_bytes_before, m_block->no_compact_free_bytes() );
    EXPECT_EQ( total_free_bytes_before + 2 * k_branchingFactor * sizeof(RecordT), m_block->total_free_bytes() );

    m_block->compact_records();

    EXPECT_EQ( m_block->no_compact_free_bytes(), m_block->total_free_bytes() );
    EXPECT_EQ( 0, m_block->compact_records_free_bytes() );
    EXPECT_EQ( total_free_bytes_before + 2 * k_branchingFactor * sizeof(RecordT), m_block->total_free_bytes() );

    for( uint32_t i = 0; i < 4 * k_branchingFactor; ++i )
    {
        if( i & 1 )
        {
            BlockT::Node* node = ( i < 2 * k_branchingFactor ) ? node_0 : node_1;
            uint32_t index = ( ( i - 1 ) / 2 ) % k_branchingFactor;
            BlockT::Pair_T pair = node->get_pair( index );
            KeyT key = pair.first;

            EXPECT_EQ( keys[ i ], key );

            BlockT::Link link = pair.second;

            EXPECT_TRUE( link.is_leaf() );
            EXPECT_TRUE( link.is_local() );
            EXPECT_EQ( m_block->index(), link.get_block() );

            RecordT record = m_block->record( link.get_local_address() );

            EXPECT_EQ( records[ i ], record );
        }
    }
}

TEST_F(TestTableWriterDataBlock, TestMigrateSubtree)
{
    BlockT::Node* node_0 = m_block->node( 0 );
    uint32_t offset_node_1 = m_block->add_node();
    BlockT::Node* node_1 = m_block->node( offset_node_1 );

    uint32_t record_offsets[ 2 * k_branchingFactor ];
    RecordT records[ 2 * k_branchingFactor ];
    KeyT keys[ 2 * k_branchingFactor ];

    for( uint32_t i = 0; i < 2 * k_branchingFactor; ++i )
    {
        uint32_t offset = m_block->add_record( RecordT(3 * i) );
        BlockT::Link link( true, true, m_block->index(), offset );
        if( i & 1 )
            node_0->insert( { KeyT(5 * i), link } );
        else
            node_1->insert( { KeyT(5 * i), link } );
    }

    m_block->add_root( offset_node_1 );

    std::shared_ptr< DataBlock > dataBlock_1 = std::make_shared< DataBlock >( k_blockSize, k_blockAlignment, k_blockIndex+1 );
    dataBlock_1->set_valid(true);
    std::shared_ptr< BlockT > block_1( new BlockT(dataBlock_1) );
    block_1->init(m_snapshot);

    {
        BlockT::Node* b1_n0 = block_1->node( 0 );
        uint32_t offset = block_1->add_record( RecordT(999) );
        BlockT::Link link( true, true, block_1->index(), offset );
        b1_n0->insert( { KeyT( 99 ), link } );
    }

    size_t total_free_bytes_before_0 = m_block->total_free_bytes();
    size_t total_free_bytes_before_1 = block_1->total_free_bytes();

    uint32_t root_offset = m_block->migrate_subtree( offset_node_1, block_1 );

    EXPECT_NE( 0, root_offset );
    EXPECT_EQ( total_free_bytes_before_0 + total_free_bytes_before_1, m_block->total_free_bytes() + block_1->total_free_bytes() );
    EXPECT_LT( total_free_bytes_before_0, m_block->total_free_bytes() );
    EXPECT_GT( total_free_bytes_before_1, block_1->total_free_bytes() );
    EXPECT_TRUE( block_1->is_root( root_offset ) );
    EXPECT_FALSE( m_block->is_root( offset_node_1 ) );

    node_1 = block_1->node( root_offset );

    for( uint32_t i = 0; i < 2 * k_branchingFactor; ++i )
    {
        if( i & 1 )
        {
            uint32_t index = (i - 1) / 2;
            BlockT::Pair_T pair = node_0->get_pair( index );
            EXPECT_EQ( KeyT(5 * i), pair.first );
            EXPECT_EQ( m_block->index(), pair.second.get_block() );
            EXPECT_EQ( RecordT(3 * i), m_block->record( pair.second.get_local_address() ) );
        }
        else
        {
            uint32_t index = i / 2;
            BlockT::Pair_T pair = node_1->get_pair( index );
            EXPECT_EQ( KeyT(5 * i), pair.first );
            EXPECT_EQ( block_1->index(), pair.second.get_block() );
            EXPECT_EQ( RecordT(3 * i), block_1->record( pair.second.get_local_address() ) );
        }
    }
}