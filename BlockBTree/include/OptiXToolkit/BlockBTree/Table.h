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
#pragma once

#include <OptiXToolkit/BlockBTree/BlockFile.h>
#include <OptiXToolkit/BlockBTree/SimpleFile.h>
#include <OptiXToolkit/Util/Exception.h>

#include <filesystem>
#include <mutex>
#include <atomic>
#include <vector>
#include <stack>
#include <memory>
#include <map>
#include <algorithm>

namespace sceneDB {

constexpr size_t get_msb( size_t num )
{
    size_t msb{ 1 };
    while( num >>= 1 )
        msb++;
    return msb;
}

constexpr size_t make_mask( size_t num )
{
    return ( size_t( 1 ) << get_msb( num ) ) - 1;
}

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
class TableWriter;

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
class TableReader;

struct Snapshot
{
    Snapshot() = default;

    ~Snapshot() = default;

    Snapshot( const Snapshot& o ) = delete;

    Snapshot( Snapshot&& o ) noexcept
        : m_snapshot_id( o.m_snapshot_id)
        , m_root_index( o.m_root_index)
        , m_new_blocks( std::move( o.m_new_blocks) )
        , m_modified_blocks( std::move( o.m_modified_blocks) )
    {
        o.m_snapshot_id = ~0ull;
        o.m_root_index = ~0ull;
    }

    void set_id( const size_t id ) { m_snapshot_id = id; }
    void set_root( const size_t root ) { m_root_index = root; }

    bool is_new( const size_t index ) { return m_new_blocks.count( index ) != 0; }
    bool is_modified( const size_t index ) { return m_modified_blocks.count( index ) != 0; }

    size_t           m_snapshot_id;
    size_t           m_root_index;
    std::set<size_t> m_new_blocks;
    std::set<size_t> m_modified_blocks;
};

bool operator< ( const Snapshot& l, const Snapshot& r ) { return l.m_snapshot_id < r.m_snapshot_id; }

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
class Table
{
    static constexpr size_t key_size = sizeof( Key );
    static constexpr size_t record_size = sizeof( Record );
    static constexpr size_t record_alignment = alignof( Record );

public:
    struct Node
    {
        static constexpr size_t leaf_bit = size_t( 1 ) << ( sizeof( size_t ) * 8 - 1 );
        static constexpr size_t local_bit = leaf_bit >> 1;
        static constexpr size_t data_mask = ~( leaf_bit | local_bit );
        static constexpr size_t local_mask = make_mask( BlockSize );
        static constexpr size_t block_mask = data_mask & ~local_mask;
        static constexpr size_t block_shift = get_msb( BlockSize );
        static constexpr size_t block_limit = ( block_mask >> block_shift ) + 1;

        // The last < Key, Link > pair in the node is a side-link.
        // The Key value is the min-key of the linked node.
        struct Link
        {
            // The two MSBs of Link values are reserved.
            // The highest bit differentiates leaf from interior nodes.
            // The next bit differentiates links within a Block (local) from
            // links to other Blocks.
            Link() = default;
            Link( bool leaf, bool local, size_t block, size_t local_address )
            {
                OTK_ASSERT( !leaf || local ); // Leaf links must be local
                set_leaf( leaf );
                set_local( local );
                set_block( block );
                set_local_address( local_address );
            }
            Link( const Link& o ) : m_link( o.m_link ) {}

            Link& operator=( const Link& o )
            {
                m_link = o.m_link;
                return *this;
            }

            bool is_valid() const { return !( m_link ^ size_t( -1 ) ); }
            void invalidate() { m_link = static_cast<size_t>( -1 ); }

            bool is_leaf() const { return m_link & leaf_bit; }
            void set_leaf( bool l )
            {
                if( l )
                    m_link |= leaf_bit;
                else
                    m_link &= ~leaf_bit;
            }

            bool is_local() const { return m_link & local_bit; }
            void set_local( bool l )
            {
                if( l )
                    m_link |= local_bit;
                else
                    m_link &= ~local_bit;
            }

            size_t get_local_address() const { return m_link & local_mask; }
            void set_local_address( size_t v )
            {
                OTK_ASSERT( v < BlockSize );
                m_link = ( m_link & ~local_mask ) | ( v & local_mask );
            }

            size_t get_block() const { return ( m_link & block_mask ) >> block_shift; }
            void set_block( size_t b )
            {
                OTK_ASSERT( b < block_limit );
                m_link = ( m_link & ~block_mask ) | ( b << block_shift );
            }

            bool operator==( const Link& o ) const
            {
                return m_link == o.m_link;
            }

            size_t m_link{ static_cast<size_t>( -1 ) };
        };

        Node()
        {
        }

        //static_assert( sizeof( std::atomic<Key> ) == sizeof( Key ), "" );
        //static_assert( std::atomic<Key>::is_always_lock_free );
        typedef std::pair< Key, Link > Pair_T;

        //Pair_T& side_link() { return m_pairs[ B - 1 ]; }
        //const Pair_T& side_link() const { return m_pairs[ B - 1 ]; }

        bool is_leaf()         const { return m_metadata.is_leaf(); }
        void set_leaf( bool l )      { m_metadata.set_leaf( l ); }

        bool is_valid()        const { return m_valid_count > 0; }
        bool is_full()         const { return m_valid_count /* + 1*/ >= B; }
        uint16_t child_count() const { return m_valid_count; }

        bool find( const Key& key, Pair_T* o_pair )
        {
            OTK_ASSERT( o_pair );
            Pair_T* start = m_pairs, end = start + m_valid_count;
            Pair_T* it = std::lower_bound( start, end, key,
                                           []( const Pair_T& e, const Key& c ) {
                                               return e.first < c;
                                           } );
            if( it == end ||
                it->first != key )
                return false;

            *o_pair = *it;
            return true;
        }

        Link traverse( const Key& key )
        {
            if( key <= low_key() )
                return m_pairs[ 0 ].second;
            if( key >= high_key() )
                return m_pairs[ m_valid_count - 1 ].second;

            Pair_T *start = m_pairs, *end = start + m_valid_count - 1;
            Pair_T* it = std::lower_bound( start, end, key,
                                           []( const Pair_T& e, const Key& c ) {
                                               return e.first < c;
                                           } );
            return it->second;
        }

        Pair_T& get_pair( uint16_t index )
        {
            OTK_ASSERT( index < m_valid_count );
            return m_pairs[ index ];
        }

        bool has_link( const Link& link );

        void update_link( const Link& link_old, const Link& link_new );

        Key low_key() const
        {
            return m_pairs[ 0 ].first;
        }

        Key high_key() const
        {
            return m_pairs[ m_valid_count - 1 ].first;
        }

        bool insert( const Pair_T& pair )
        {
            OTK_ASSERT( !is_full() );
            Pair_T *start = m_pairs, *end = start + m_valid_count;
            Pair_T* it = std::lower_bound( start, end, pair.first,
                                           []( const Pair_T& e, const Key& c ) {
                                               return e.first < c;
                                           } );
            bool exists = ( it != end &&
                            it->first == pair.first );

            if( exists )
                it->second = pair.second;
            else if( !is_full() )
            {
                for( Pair_T* src = end; src > it; --src )
                    *src = *(src - 1);
                m_valid_count++;
                *it = pair;
            }
            else
                return false;
            return true;
        }

        bool erase( const Pair_T& pair )
        {
            Pair_T *start = m_pairs, *end = start + m_valid_count;
            Pair_T* it = std::lower_bound( start, end, pair.first,
                                           []( const Pair_T& e, const Key& c ) {
                                               return e.first < c;
                                           } );
            bool exists = ( it != end &&
                            it->first == pair.first );

            if( !exists )
                return false;

            for( Pair_T* src = it; src < end; ++src )
                *src = *(src + 1);
            m_valid_count--;

            return true;
        }

        void erase( uint16_t index )
        {
            OTK_ASSERT( index < m_valid_count );
            if( index + 1 == m_valid_count )
            {
                // Easy. Don't need to move anything.
                m_valid_count--;
                return;
            }

            for( Pair_T* src = m_pairs + index; src < m_pairs + m_valid_count; ++src )
                *src = *(src + 1);
            m_valid_count--;
        }

        void clear()
        {
            m_valid_count = 0;
        }

        void update_block( const size_t old_block, const size_t new_block )
        {
            for( uint16_t i = 0; i < m_valid_count; ++i )
                if( m_pairs[ i ].second.get_block() == old_block )
                    m_pairs[ i ].second.set_block( new_block );
        }

        /*bool split(Node* right_node)
        {
            if( !is_full() )
                return false;
            right_node->m_metadata.set_leaf( is_leaf() );
            for( Pair_T* src = m_pairs + (m_valid_count >> 1), dst = right_node->m_pairs; src != m_pairs + m_valid_count; src++, dst++ )
                *dst = *src;
            right_node->m_valid_count = m_valid_count - ( m_valid_count >> 1 );
            m_valid_count -= m_valid_count >> 1;
        }*/

        /*bool tryLatch()
        {
            return m_latch.compare_exchange_weak( false, true );
        }

        void latch()
        {
            while( !tryLatch() )
            {
            }
        }

        void unlatch()
        {
            m_latch.store( false );
        }*/

        Pair_T   m_pairs[ B ];
        uint16_t m_valid_count{ 0 };
        Link     m_metadata;
        //std::atomic<bool> m_latch{ false };
    };

    typedef TableWriter<Key, Record, B, BlockSize, BlockAlignment> TableWriterType;
    typedef TableReader<Key, Record, B, BlockSize, BlockAlignment> TableReaderType;

    Table( const char* directory, const char* tableName )
        : m_tableName( tableName )
        , m_directory( directory )
        , m_dataFileName( std::filesystem::path( directory ) / std::filesystem::path( tableName ).replace_extension( "dat" ) )
        , m_snapshotFileName( std::filesystem::path( directory ) / std::filesystem::path( tableName ).replace_extension( "snap" ) )
    {
        m_dataFileHeader.reset( new Header() );
    }

    /// Close a Table.  The contents of the table persist until it is destroyed via the destroy() method.
    ~Table() = default;

    /// Get a TableWriter that can be used to insert records in the store.
    /// The Table must have been initialized with write access.
    /// Subsequent calls return the same writer. 
    /// The writer can be used concurrently by multiple threads. 
    /// Throws an exception if an error occurs.
    std::shared_ptr< TableWriterType > getWriter();

    /// Get a TableReader that can be used to read records from the table.
    /// Subsequent calls (with the same Snapshot argument) return the same reader,
    /// which can be used concurrently by multiple threads. 
    /// Throws an exception if an error occurs.
    std::shared_ptr< TableReaderType > getReader( std::shared_ptr< Snapshot > snapshot );

    /// Get the table name.
    const std::string& getTableName() const { return m_tableName; }

    /// Check whether the table exists on disk.
    bool exists() const { return std::filesystem::exists( m_dataFileName ); }

    /// Check whether we can write to the Table.
    bool isWriteable() const { if( m_dataFile ) return m_dataFile->isWriteable(); return false; }

    /// Create the Table on disk and initialize it.
    void init( bool request_write );

    /// Close the table, releasing any reader and writer instances.
    void close();

    /// Destroy the table, removing any associated disk files.  Any previously created writers or
    /// readers should be destroyed before calling destroy().
    void destroy();

    const std::filesystem::path& getDataFile() const { return m_dataFileName; }
    const std::filesystem::path& getSnapshotFile() const { return m_snapshotFileName; }

private:
    class Header : public BlockFile::Header
    {
    public:
        Header() = default;

        virtual bool check( const BlockFile::Header* header ) const override final
        {
            const Header* h = dynamic_cast< const Header* >( header );
            OTK_ASSERT( h );

            return m_keySize == h->m_keySize &&
                m_recordSize == h->m_recordSize &&
                BlockFile::Header::check( header );
        }

        virtual size_t getSize() const override final
        {
            return 2 * sizeof( size_t ) + BlockFile::Header::getSize();
        }

        virtual void serialize( void* buf ) const override final
        {
            size_t* _buf = ( size_t* )buf;
            _buf[ 0 ] = m_keySize;
            _buf[ 1 ] = m_recordSize;
            BlockFile::Header::serialize( ( char* )( buf )+2 * sizeof( size_t ) );
        }

        virtual void deserialize( void* buf ) override final
        {
            size_t* _buf = ( size_t* )buf;
            m_keySize = _buf[ 0 ];
            m_recordSize = _buf[ 1 ];
            BlockFile::Header::deserialize( ( char* )( buf )+2 * sizeof( size_t ) );
        }

        virtual std::unique_ptr<BlockFile::Header> getInstance() const override final
        {
            return std::unique_ptr<BlockFile::Header>( new Header() );
        }

        size_t m_keySize{ key_size };
        size_t m_recordSize{ record_size };
    };

    void initSnapshots();
    void writeSnapshots() const;

    const std::string           m_tableName;
    const std::filesystem::path m_directory;
    const std::filesystem::path m_dataFileName;
    const std::filesystem::path m_snapshotFileName;

    std::shared_ptr< BlockFile >  m_dataFile;
    std::shared_ptr< Header >     m_dataFileHeader;
    std::shared_ptr< SimpleFile > m_snapshotFile;

    std::shared_ptr< Snapshot > m_latestSnapshot;

    std::mutex m_mutex;
    std::shared_ptr< TableWriterType > m_writer;
    std::map< std::shared_ptr< const Snapshot >, std::shared_ptr< TableReaderType > > m_readers;
};

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
bool Table<Key, Record, B, BlockSize, BlockAlignment>::Node::has_link( const Link& link )
{
    for( uint16_t i = 0; i < m_valid_count; ++i )
        if( m_pairs[i].second == link )
            return true;
    return false;
}

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
void Table<Key, Record, B, BlockSize, BlockAlignment>::Node::update_link( const Link& link_old, const Link& link_new )
{
    for( uint16_t i = 0; i < m_valid_count; ++i )
        if( m_pairs[i].second == link_old )
        {
            m_pairs[i].second = link_new;
        }
}

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
class TableWriter
{
    static constexpr size_t key_size    = sizeof( Key );
    static constexpr size_t record_size = sizeof( Record );

public:
    typedef Table< Key, Record, B, BlockSize, BlockAlignment > TableType;
    typedef typename TableType::Node         Node;
    typedef typename TableType::Node::Link   Link;
    typedef typename TableType::Node::Pair_T Pair_T;

private:
    class TableDataBlock
    {
    public:
        struct Metadata
        {
            int m_next_node{ 0 };
            int m_next_record{ 0 };
            int m_record_count{ 0 };
            std::set< int > m_free_nodes;
            std::set< int > m_free_records;
        };

        struct RootData
        {
            void add_root( uint32_t offset ) { m_root_offsets[ m_root_count++ ] = offset; }
            void delete_root( uint32_t offset )
            {
                for( uint32_t i = 0; i < m_root_count; ++i )
                    if( m_root_offsets[ i ] == offset )
                    {
                        m_root_offsets[ i ] = m_root_offsets[ --m_root_count ];
                        return;
                    }
            }
            bool is_root(uint32_t offset) const
            {
                for( uint32_t i = 0; i < m_root_count; ++i )
                    if( m_root_offsets[ i ] == offset )
                    {
                        return true;
                    }
                return false;
            }

            uint32_t m_root_offsets[ B ];
            uint32_t m_root_count{ 0 };
        };

        TableDataBlock( std::shared_ptr< DataBlock > data )
            : m_dataBlock( data )
        {
            OTK_ASSERT( m_dataBlock->size == BlockSize );
            OTK_ASSERT( usable_size() >= ( B * sizeof( Record ) + sizeof( Node ) ) );
        }

        void release()
        {
            m_dataBlock.reset();
        }

        bool is_valid() const
        {
            return bool( m_dataBlock.get() );
        }

        void set_data( std::shared_ptr< DataBlock > data )
        {
            OTK_ASSERT( data->size == BlockSize );
            m_dataBlock = data;
        }

        void compact_nodes( Node* parent )
        {
            RootData& rd = *root_data_ptr();
            std::map< int, int > remap;

            while( !m_metaData.m_free_nodes.empty() )
            {
                int offset_new = m_metaData.m_free_nodes.extract( m_metaData.m_free_nodes.begin() ).value();
                int offset_old = m_metaData.m_next_node - sizeof( Node );

                *node( offset_new ) = *node( offset_old );

                bool is_root = false;
                for( int i = 0; i < rd.m_root_count; ++i )
                    if( rd.m_root_offsets[ i ] == offset_old )
                    {
                        is_root = true;
                        rd.m_root_offsets[ i ] = offset_new;

                        if( parent )
                            parent->update_link( Link( false, false, index(), offset_old ), Link( false, false, index(), offset_new ) );

                        break;
                    }

                if( !is_root )
                    remap.insert( { offset_old, offset_new } );

                m_metaData.m_next_node -= sizeof( Node );
                while( m_metaData.m_free_nodes.count( m_metaData.m_next_node - sizeof( Node ) ) )
                {
                    // Advance through contiguous free space.
                    m_metaData.m_next_node -= sizeof( Node );
                    m_metaData.m_free_nodes.erase( m_metaData.m_free_nodes.find( m_metaData.m_next_node ) );
                }
            }

            if( remap.empty() )
                return;

            for( int offset = 0; offset < m_metaData.m_next_node && !remap.empty(); offset += sizeof( Node ) )
            {
                for( auto it = remap.begin(); it != remap.end(); ++it )
                {
                    Link link_old( true, true, index(), it->first );

                    if( node( offset )->has_link( link_old ) )
                    {
                        Link link_new( true, true, index(), it->second );   
                        node( offset )->update_link( link_old, link_new );
                        remap.erase( it );
                        break;
                    }
                }
            }
        }

        void compact_records()
        {
            std::map< int, int > remap;

            while( !m_metaData.m_free_records.empty() )
            {
                int offset_new = m_metaData.m_free_records.extract( std::prev( m_metaData.m_free_records.end() ) ).value();
                int offset_old = m_metaData.m_next_record + sizeof( Record );

                record( offset_new ) = record( offset_old );

                remap.insert( { offset_old, offset_new } );

                m_metaData.m_next_record += sizeof( Record );
                while( m_metaData.m_free_records.count( m_metaData.m_next_record + sizeof( Record ) ) )
                {
                    // Advance through contiguous free space.
                    m_metaData.m_next_record += sizeof( Record );
                    m_metaData.m_free_records.erase( m_metaData.m_free_records.find( m_metaData.m_next_record ) );
                }
            }

            if( remap.empty() )
                return;

            for( int offset = 0; offset < m_metaData.m_next_node && !remap.empty(); offset += sizeof( Node ) )
            {
                // Skip free nodes.
                if( m_metaData.m_free_nodes.count( offset ) ||
                    !node( offset )->is_leaf() )
                    continue;
                for( auto it = remap.begin(); it != remap.end(); ++it )
                {
                    Link link_old( true, true, index(), it->first );

                    if( node( offset )->has_link( link_old ) )
                    {
                        Link link_new( true, true, index(), it->second );   
                        node( offset )->update_link( link_old, link_new );
                        remap.erase( it );
                    }
                }
            }
        }
        
        size_t compact_records_free_bytes() const
        {
            return m_metaData.m_free_records.size() * sizeof( Record );
        }

        size_t compact_nodes_free_bytes() const
        {
            return m_metaData.m_free_nodes.size() * sizeof( Node );
        }

        size_t no_compact_free_bytes() const
        {
            return m_metaData.m_next_record + sizeof( Record ) - m_metaData.m_next_node;
        }

        size_t total_free_bytes() const
        {
            return compact_nodes_free_bytes() + compact_records_free_bytes() + no_compact_free_bytes();
        }

        template< size_t size >
        bool has_room_for_n( uint32_t n, bool& need_node_compaction, bool& need_record_compaction ) const
        {
            need_node_compaction   = false;
            need_record_compaction = false;

            auto nc_fb = no_compact_free_bytes();

            if( nc_fb >= n * size )
                return true;

            auto cr_fb = compact_records_free_bytes();
            if( nc_fb + cr_fb >= n * size )
            {
                need_record_compaction = true;
                return true;
            }

            auto cn_fb = compact_nodes_free_bytes();
            if( nc_fb + cn_fb >= n * size )
            {
                need_node_compaction = true;
                return true;
            }

            if( total_free_bytes() >= n * size )
            {
                need_node_compaction   = true;
                need_record_compaction = true;
                return true;
            }

            return false;
        }

        bool has_room_for_n_nodes( uint32_t n, bool& need_node_compaction, bool& need_record_compaction ) const
        {
            return has_room_for_n< sizeof(Node) >( n, need_node_compaction, need_record_compaction );
        }

        bool has_room_for_n_records( uint32_t n, bool& need_node_compaction, bool& need_record_compaction ) const
        {
            return has_room_for_n< sizeof(Record) >( n, need_node_compaction, need_record_compaction );
        }

        uint32_t add_node()
        {
            int result = -1;

            if( m_metaData.m_free_nodes.size() )
            {
                result = m_metaData.m_free_nodes.extract( m_metaData.m_free_nodes.begin() ).value();
            }
            else if( no_compact_free_bytes() >= sizeof( Node ) )
            {
                result = m_metaData.m_next_node;
                m_metaData.m_next_node += sizeof( Node );
            }

            if( result >= 0 )
            {
                new ( node( result ) ) Node();
            }

            return uint32_t( result );
        }

        void remove_node( const uint32_t offset )
        {
            OTK_ASSERT( offset < usable_size() );
            if( offset >= m_metaData.m_next_node )
                return;

            if( m_metaData.m_next_node - sizeof( Node ) == offset )
            {
                m_metaData.m_next_node -= sizeof( Node );
                // Coalesce free space at the end.
                while( m_metaData.m_free_nodes.count( m_metaData.m_next_node - sizeof( Node ) ) )
                {
                    m_metaData.m_next_node -= sizeof(Node);
                    m_metaData.m_free_nodes.erase( m_metaData.m_free_nodes.find( m_metaData.m_next_node ) );
                }
            }
            else
                m_metaData.m_free_nodes.insert( offset );
        }

        uint32_t add_record( const Record& new_record )
        {
            int result = -1;

            if( m_metaData.m_free_records.size() )
            {
                result = m_metaData.m_free_records.extract( std::prev( m_metaData.m_free_records.end() ) ).value();
            }
            else if( no_compact_free_bytes() >= sizeof( Record ) )
            {
                result = m_metaData.m_next_record;
                m_metaData.m_next_record -= sizeof( Record );
            }

            if( result >= 0 )
            {
                record( result ) = new_record;
                m_metaData.m_record_count++;
            }

            return uint32_t( result );
        }

        void remove_record( const uint32_t offset )
        {
            OTK_ASSERT( offset < usable_size() );
            if( offset <= m_metaData.m_next_record )
                return;

            m_metaData.m_record_count--;

            if( m_metaData.m_next_record - sizeof( Record ) == offset )
            {
                m_metaData.m_next_record += sizeof( Record );
                // Coalesce free space at the end.
                while( m_metaData.m_free_records.count( m_metaData.m_next_record + sizeof( Record ) ) )
                {
                    m_metaData.m_next_record += sizeof( Record );
                    m_metaData.m_free_records.erase( m_metaData.m_free_records.find( m_metaData.m_next_record) );
                }
            }
            else
                m_metaData.m_free_records.insert( offset );
        }

        void add_root( const uint32_t offset )
        {
            auto rd = root_data_ptr();
            OTK_ASSERT( rd->m_root_count < B );
            rd->m_root_offsets[ rd->m_root_count++ ] = offset;
        }

        void remove_root( const uint32_t offset )
        {
            auto rd = root_data_ptr();
            uint32_t index = ~0u;
            for( uint32_t i = 0; i < rd->m_root_count; ++i )
                if( rd->m_root_offsets[ i ] == offset )
                {
                    index = i;
                    break;
                }

            if( index != ~0u )
            {
                rd->m_root_offsets[ index ] = rd->m_root_offsets[ --rd->m_root_count ];
            }
        }

        uint32_t migrate_subtree( uint32_t src_root_offset, std::shared_ptr<TableDataBlock> dst_block )
        {
            uint32_t dst_root_offset = -1;
            
            // Check for newly minted block, which already has a single (but empty) node.
            if( dst_block->root_count() == 1 &&
                dst_block->node( dst_block->root_offset( 0 ) )->child_count() == 0 )
                dst_root_offset = dst_block->root_offset( 0 );

            if( dst_root_offset == uint32_t( -1 ) )
            {
                dst_root_offset = dst_block->add_node();
                dst_block->add_root( dst_root_offset );
            }

            OTK_ASSERT( dst_root_offset != uint32_t( -1 ) );

            std::stack< std::pair< uint32_t, uint32_t > > s;
            s.push( { src_root_offset, dst_root_offset } );

            do
            {
                auto offset_pair = s.top();
                s.pop();

                Node* src_node = node( offset_pair.first );
                Node* dst_node = dst_block->node( offset_pair.second );

                dst_node->set_leaf( src_node->is_leaf() );

                for( uint16_t j = 0; j < src_node->child_count(); ++j )
                {
                    Pair_T& src_pair = src_node->get_pair( j );
                    Pair_T  dst_pair = src_pair;

                    if( src_pair.second.is_local() )
                    {
                        dst_pair.second.set_block( dst_block->index() );
                        if( src_node->is_leaf() )
                        {
                            OTK_ASSERT( src_pair.second.is_leaf() );
                            uint32_t offset = dst_block->add_record( record( src_pair.second.get_local_address() ) );
                            OTK_ASSERT( offset != uint32_t( -1 ) );
                            dst_pair.second.set_local_address( offset );
                            remove_record( src_pair.second.get_local_address() );
                        }
                        else
                        {
                            OTK_ASSERT( !src_pair.second.is_leaf() );
                            uint32_t offset = dst_block->add_node();
                            OTK_ASSERT( offset != uint32_t( -1 ) );
                            dst_pair.second.set_local_address( offset );
                            s.push( { uint32_t( src_pair.second.get_local_address() ), offset } );
                        }
                    }

                    dst_node->insert( dst_pair );
                }

                remove_node( offset_pair.first );

            } while (!s.empty());

            return dst_root_offset;
        }

        void init( const Snapshot& snapshot )
        {
            set_snapshot( snapshot );
            new ( root_data_ptr() ) RootData();

            auto offset = add_node();
            OTK_ASSERT( offset == 0 );
            add_root( offset );

            m_metaData.m_next_record = record_offset();
        }

        void init_existing()
        {
            const auto& root_data = *root_data_ptr();
            
            std::set< uint32_t > nodes;
            std::set< uint32_t > records;

            for( uint32_t i = 0; i < root_data.m_root_count; ++i )
            {
                std::stack< uint32_t > s;
                s.push( root_data.m_root_offsets[ i ] );

                do
                {
                    uint32_t offset = s.top();
                    s.pop();

                    nodes.insert( offset );

                    Node* n = node( offset );
                    for( uint16_t j = 0; j < n->child_count(); ++j )
                    {
                        Link& link = n->get_pair( j ).second;
                        if( n->is_leaf() )
                            records.insert( link.get_local_address() );
                        else if( link.is_local() )
                            s.push( link.get_local_address() );

                        if( link.is_local() )
                            link.set_block( index() );
                    }
                } while( !s.empty() );
            }

            OTK_ASSERT( nodes.size() );
            m_metaData.m_next_node   = *nodes.rbegin() + sizeof( Node );
            m_metaData.m_next_record = *records.begin() - sizeof( Record );

            uint32_t curr = 0;
            for( uint32_t offset : nodes )
            {
                while( curr < offset )
                {
                    m_metaData.m_free_nodes.insert( curr );
                    curr += sizeof( Node );
                }
                curr += sizeof( Node );
            }

            curr = *records.begin();
            for( uint32_t offset : records )
            {
                while( curr < offset )
                {
                    m_metaData.m_free_records.insert( curr );
                    curr += sizeof( Record );
                }
                curr += sizeof( Record );
            }
        }

        uint32_t root_count() const
        {
            return root_data_ptr()->m_root_count;
        }

        size_t root_offset( uint32_t root_index ) const
        {
            OTK_ASSERT( root_index < root_count() );
            return root_data_ptr()->m_root_offsets[ root_index ];
        }

        bool is_root( uint32_t offset ) const
        {
            return root_data_ptr()->is_root( offset );
        }

        void set_snapshot( const Snapshot& snapshot )
        {
            snapshot_id() = snapshot.m_snapshot_id;
        }

        Node* node( const size_t offset ) { OTK_ASSERT( offset < m_metaData.m_next_node ); return node_ptr( offset ); }
        const Node* node( const size_t offset ) const { OTK_ASSERT( offset < m_metaData.m_next_node ); return node_ptr( offset ); }

        Record& record( const size_t offset ) { OTK_ASSERT( offset >= m_metaData.m_next_record ); return *record_ptr( offset ); }
        const Record& record( const size_t offset ) const { OTK_ASSERT( offset >= m_metaData.m_next_record ); return *record_ptr( offset ); }

        static constexpr size_t size() { return BlockSize; }
        size_t usable_size()   const { return root_data_offset(); }

        bool is_full() const { return total_free_bytes() >= ( m_metaData.m_record_count ? std::max( sizeof( Node ), sizeof( Record ) ) : sizeof( Node ) ); }

        size_t index() const { return m_dataBlock->index; }

    private:
        static constexpr uint32_t snap_id_offset()   { return size() - sizeof( std::size_t ); }
        static constexpr uint32_t root_data_offset() { return snap_id_offset() - sizeof( RootData ); }
        static constexpr uint32_t record_offset()    { return root_data_offset() - sizeof( Record ) - ( ( root_data_offset() - sizeof( Record ) ) % alignof( Record ) ); }

        char*       ptr()       { return ( char* )( m_dataBlock->get_data() ); }
        const char* ptr() const { return ( const char* )( m_dataBlock->get_data() ); }

        std::size_t* snap_id_ptr() { return ( size_t* )( ptr() + snap_id_offset() ); }

        RootData*       root_data_ptr()       { return reinterpret_cast< RootData* >( ptr() + root_data_offset() ); }
        const RootData* root_data_ptr() const { return reinterpret_cast< const RootData* >( ptr() + root_data_offset() ); }

        Node*       node_ptr( const size_t offset )       { return reinterpret_cast< Node* >( ptr() + offset ); }
        const Node* node_ptr( const size_t offset ) const { return reinterpret_cast< const Node* >( ptr() + offset ); }

        Record*       record_ptr( const size_t offset )       { return reinterpret_cast< Record* >( ptr() + offset ); }
        const Record* record_ptr( const size_t offset ) const { return reinterpret_cast< const Record* >( ptr() + offset ); }

        size_t& snapshot_id() { return *snap_id_ptr(); }

        std::shared_ptr< DataBlock > m_dataBlock;
        Metadata                     m_metaData;
        //std::condition_variable      m_cv;
        //std::mutex                   m_mutex;
        //std::atomic_uint32_t         m_ref_count;
    };

    typedef std::shared_ptr<TableDataBlock> TableDataBlockPtr;

public:
    TableWriter( std::shared_ptr< BlockFile > data_file, std::shared_ptr< const Snapshot > latest_snapshot )
        : m_dataFile( data_file )
    {
        if( latest_snapshot )
        {
            m_currentSnapshot.set_id( latest_snapshot->m_snapshot_id + 1 );
            m_currentSnapshot.set_root( latest_snapshot->m_root_index );
        }
        else
        {
            m_currentSnapshot.set_id( 0 );
            m_currentSnapshot.set_root( 0 );
        }
    }

    /*bool traverse_side_links(Node*& curr_node, TableDataBlockPtr curr_block, Link& current, const Key& key)
    {
        bool link_used = false;
        while( curr_node->side_link().first && key >= curr_node->side_link().first )
        {
            if( current.getBlock() != curr_node->side_link().second.getBlock() )
                curr_block = get_block( curr_node->side_link().second.getBlock() );

            current = curr_node->side_link().second;
            curr_node = curr_block->node( current.getLocalAddress() );
            link_used = true;
        }
        return link_used;
    }*/

    void Insert( const Key& key, const Record& record )
    {
        // Non-concurrent version.         
        Link root_link         = get_root_link();
        Link block_parent_link = Link();
        Link parent_link       = Link();
        Link current_link      = root_link;

        TableDataBlockPtr block = get_root_block();

        while( true )
        {
            if( block->is_full() )
            {
                Node* block_parent_node = nullptr;
                if( block_parent_link.is_valid() )
                {
                    block_parent_node = get_block( block_parent_link.get_block() )->node( block_parent_link.get_local_address() );
                }

                split_block( block, block_parent_node );

                parent_link = block_parent_link;
                if( block_parent_node )
                    current_link = block_parent_node->traverse( key );
                else
                    current_link = root_link;

                block = get_block( current_link.get_block() );
                continue;
            }

            Node* current_node = block->node( current_link.get_local_address() );
            if( current_node->is_full() )
            {
                Node* block_parent_node = nullptr;
                if( block_parent_link.is_valid() )
                {
                    block_parent_node = get_block( block_parent_link.get_block() )->node( block_parent_link.get_local_address() );
                }

                Node* parent_node = nullptr;
                if( parent_link.is_valid() )
                {
                    parent_node = get_block( parent_link.get_block() )->node( parent_link.get_local_address() );
                }

                if( !split_node( block, parent_node, block_parent_node, current_link ) )
                {
                    // Had to split the block internally, or shuffle things around.
                    // Need to restart from the block_parent (or root).
                    parent_link = block_parent_link;
                    if( block_parent_node )
                        current_link = block_parent_node->traverse( key );
                    else
                        current_link = root_link;

                    block = get_block( current_link.get_block() );
                    continue;
                }

                if( parent_node )
                {
                    current_link = parent_node->traverse( key );
                    block = get_block( current_link.get_block() );
                    current_node = block->node( current_link.get_local_address() );
                }
                else
                {
                    OTK_ASSERT( current_link == root_link );
                    OTK_ASSERT( block->index() == current_link.get_block() );
                    OTK_ASSERT( block->node( current_link.get_local_address() ) == current_node );
                }
            }

            if( current_node->is_leaf() )
            {
                // If we split the a node, block could have filled up, in which case
                // we go around the loop again to split it.
                if( block->is_full() )
                    continue;

                uint32_t offset = block->add_record( record );
                OTK_ASSERT( offset != uint32_t( -1 ) );

                bool result = current_node->insert( std::make_pair( key, Link( true, true, block->index(), offset ) ) );
                OTK_ASSERT( result );
                return;
            }

            Link next_link = current_node->traverse( key );
            if( next_link.get_block() != block->index() )
            {
                block_parent_link = current_link;
                block = get_block( next_link.get_block() );
            }

            parent_link  = current_link;
            current_link = next_link;            
        }
    }

    void Erase( const Key& key )
    {

    }

    Snapshot&& TakeSnaphot()
    {
        m_dataFile->flush( false );

        Snapshot snapshot( std::move( m_currentSnapshot ) );

        m_currentSnapshot.setId( snapshot.m_snapshotId + 1 );
        m_currentSnapshot.setRoot( snapshot.m_root_index );

        m_root_link.invalidate();

        return std::move( snapshot );
    }

    // No copying
    TableWriter( const TableWriter& o ) = delete;
    
private:
    void split_block( TableDataBlockPtr block, Node* block_parent )
    {
        bool skip_first = false;
        Node* _parent   = nullptr;

        if( !block_parent || 
            block->root_count() == 1 )
        {
            _parent = block->node( block->root_offset( 0 ) );
        }
        else
        {
            OTK_ASSERT( block_parent );
            _parent    = block_parent;
            skip_first = true; // Skip first subtree, which remains in the same block.
        }

        OTK_ASSERT( _parent );
        auto child_count = _parent->child_count();

        for( uint32_t i = 0; i < child_count; ++i )
        {
            Pair_T& child_pair = _parent->get_pair( i );

            if( child_pair.second.get_block() != block->index() )
                continue;

            if( skip_first )
            {
                skip_first = false;
                continue;
            }

            if( block_parent )
                OTK_ASSERT( block->is_root( child_pair.second.get_local_address() ) );

            TableDataBlockPtr new_block = get_new_block();
            size_t offset = block->migrate_subtree( child_pair.second.get_local_address(), new_block );

            child_pair.second.set_local( false );
            child_pair.second.set_block( new_block->index() );
            child_pair.second.set_local_address( offset );
        }
    }

    // Returns whether the split succeeded or not.
    // Split may not succeed if the block needs to be split,
    // in which case traversal needs to be restarted from the
    // block's parent (as nodes can move).
    bool split_node( TableDataBlockPtr block, Node* parent, Node* block_parent, Link src_link )
    {
        OTK_ASSERT( src_link.get_local_address() == block->index() );
        Node* src_node = block->node( src_link.get_local_address() );
        OTK_ASSERT( src_node->is_full() );

        if( !parent )
        {
            OTK_ASSERT( !block_parent );
            OTK_ASSERT( block->is_root( src_link.get_local_address() ) );
            // Root node case. Keep src_node as the root node.
            // Add two nodes, migrate half the data to each.
            bool need_node_compaction = false, need_record_compaction = false;
            if( !block->has_room_for_n_nodes( 2, need_node_compaction, need_record_compaction ) )
            {
                split_block( block, nullptr );
            }
            
            if( need_record_compaction )
                block->compact_records();
            if( need_node_compaction )
            {
                block->compact_nodes( nullptr );
                // Pretty certain that node compaction can't move the Table's root.
                OTK_ASSERT( block->root_offset( 0 ) == src_link.get_local_address() );
            }

            OTK_ASSERT( block->has_room_for_n_nodes( 2, need_node_compaction, need_record_compaction ) );

            uint32_t left_offset  = block->add_node();
            uint32_t right_offset = block->add_node();

            Node* left_node  = block->node( left_offset );
            Node* right_node = block->node( right_offset );

            left_node->set_leaf( src_node->is_leaf() );
            right_node->set_leaf( src_node->is_leaf() );

            uint16_t count = src_node->child_count();
            uint16_t left_count = count >> 1;

            for( uint16_t i = 0; i < left_count; ++i )
                left_node->insert( src_node->get_pair( i ) );
            for( uint16_t i = left_count; i < count; ++i )
                right_node->insert( src_node->get_pair( i ) );

            src_node->clear();
            src_node->set_leaf( false );

            src_node->insert( {  left_node->low_key(), Link( false, true, block->index(), left_offset ) } );
            src_node->insert( { right_node->low_key(), Link( false, true, block->index(), right_offset ) } );
        }
        else
        {
            // Non-root case.
            bool need_node_compaction = false, need_record_compaction = false;
            if( !block->has_room_for_n_nodes( 1, need_node_compaction, need_record_compaction ) )
            {
                split_block( block, block_parent );
                return false;
            }
            
            if( need_node_compaction )
            {
                block->compact_nodes( block_parent );
                return false;
            }
            if( need_record_compaction )
                block->compact_records();

            OTK_ASSERT( block->has_room_for_n_nodes( 1, need_node_compaction, need_record_compaction ) );

            uint16_t count = src_node->child_count();
            uint16_t left_count = count >> 1;

            // Check to see if splitting the node would result in two
            // nodes pointing to the same block, which can't happen.
            // If so, migrate some subtrees to a new block.
            if( !src_node->get_pair( left_count - 1 ).second.is_local() &&
                src_node->get_pair( left_count - 1 ).second.get_block() == src_node->get_pair( left_count ).second.get_block() )
            {
                size_t block_index = src_node->get_pair( left_count ).second.get_block();
                OTK_ASSERT( block_index != block->index() );
                uint16_t left_n = 0, right_n = 0;

                for( uint16_t i = 0; i < count; ++i )
                    if( src_node->get_pair( i ).second.get_block() == block_index )
                    {
                        if (i < left_count)
                            left_n++;
                        else
                            right_n++;
                    }

                TableDataBlockPtr new_block = get_new_block();

                uint16_t start = 0;
                uint16_t end   = left_count;
                if( left_n > right_n )
                {
                    start = left_count;
                    end   = count;
                }

                TableDataBlockPtr src_block = get_block( block_index );
                // Check for copy_on_write of src_block
                if( src_block->index() != block_index )
                {
                    src_node->update_block( block_index, src_block->index() );
                    block_index = src_block->index();
                }

                for( uint16_t i = start; i < end; ++i )
                {
                    Pair_T& pair = src_node->get_pair( i );
                    if( pair.second.get_block() != block_index )
                        continue;

                    size_t offset = src_block->migrate_subtree( pair.second.get_local_address(), new_block );

                    pair.second.set_local( false );
                    pair.second.set_block( new_block->index() );
                    pair.second.set_local_address( offset );
                }
            }

            // Now add a new node and move half of this node to it.
            uint32_t right_offset = block->add_node();
            Node* right_node      = block->node(right_offset);

            right_node->set_leaf( src_node->is_leaf() );

            for( uint16_t i = left_count; i < count; ++i )
                right_node->insert( src_node->get_pair( i ) );

            for( uint16_t i = count - 1; i >= left_count; --i )
                src_node->erase( i );

            parent->insert( { right_node->low_key(), Link( false, true, block->index(), right_offset ) } );
        }

        return true;
    }

    size_t copy_on_write( const size_t block_index )
    {
        auto old_block = m_dataFile->checkOutForRead( block_index );
        auto new_block = m_dataFile->checkOutNewBlock();

        *new_block = *old_block;
        TableDataBlockPtr block = std::make_shared< TableDataBlock >( new_block );
        block->set_snapshot( m_currentSnapshot );
        block->init_existing();
        block->compact_records(); // Can't compact nodes as we would need to know the parent.

        m_currentSnapshot.m_modified_blocks.insert( old_block->index );
        m_currentSnapshot.m_new_blocks.insert( new_block->index );

        m_blockMap[ new_block->index ] = block;

        return new_block->index;
    }

    TableDataBlockPtr get_block( size_t index )
    {
        OTK_ASSERT( m_dataFile->exists( index ) );
        if( !m_currentSnapshot.is_new( index ) )
        {
            index = copy_on_write( index );
        }
        else if( !m_blockMap.count( index ) )
        {
            m_blockMap.emplace( std::make_pair( index, std::make_shared< TableDataBlock >( m_dataFile->checkOutForReadWrite( index ) ) ) );
            m_blockMap[ index ]->init_existing();
        }

        return m_blockMap.at( index );
    }

    TableDataBlockPtr get_new_block()
    {
        TableDataBlockPtr new_block = std::make_shared< TableDataBlock >( m_dataFile->checkOutNewBlock() );
        new_block->init( m_currentSnapshot );
        m_currentSnapshot.m_new_blocks.insert( new_block->index() );
        m_blockMap[ new_block->index() ] = new_block;
        return new_block;
    }

    TableDataBlockPtr get_root_block()
    {
        if( !m_dataFile->exists( m_currentSnapshot.m_root_index ) )
        {
            TableDataBlockPtr new_block = get_new_block();
            OTK_ASSERT( new_block->index() == m_currentSnapshot.m_root_index );
            Node* root = new_block->node( new_block->root_offset( 0 ) );
            root->set_leaf( true );
        }
        else if( !m_currentSnapshot.is_new( m_currentSnapshot.m_root_index ) )
        {
            m_currentSnapshot.set_root( copy_on_write( m_currentSnapshot.m_root_index ) );
        }
        else if( !m_blockMap.count( m_currentSnapshot.m_root_index ) )
        {
            m_blockMap.emplace( m_currentSnapshot.m_root_index, std::make_shared< TableDataBlock >( m_dataFile->checkOutForReadWrite( m_currentSnapshot.m_root_index ) ) );
            m_blockMap[ m_currentSnapshot.m_root_index ]->init_existing();
        }

        return m_blockMap.at( m_currentSnapshot.m_root_index );
    }

    Link get_root_link()
    {
        if( !m_root_link.is_valid() )
        {
            TableDataBlockPtr root_block = get_root_block();

            m_root_link.set_local( false );
            m_root_link.set_leaf( false );
            m_root_link.set_block( m_currentSnapshot.m_root_index );
            m_root_link.set_local_address( root_block->root_offset( 0 ) );
        }

        return m_root_link;
    }

    Link                                  m_root_link;
    Snapshot                              m_currentSnapshot;
    std::map< size_t, TableDataBlockPtr > m_blockMap;
    std::shared_ptr< BlockFile >          m_dataFile;
    //std::mutex                            m_mutex;
};

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
void Table< Key, Record, B, BlockSize, BlockAlignment >::init( bool request_write )
{
    std::unique_lock<std::mutex> lock( m_mutex );

    if( !m_dataFile )
    {
        // Create the specified directory if necessary. (Throws if an error occurs.)
        std::filesystem::create_directory( m_directory );

        m_dataFile.reset( new BlockFile( getDataFile(), m_dataFileHeader, BlockSize, BlockAlignment, request_write ) );
        m_snapshotFile.reset( new SimpleFile( getSnapshotFile(), request_write ) );

        initSnapshots();
    }
    else
        OTK_ASSERT( m_snapshotFile );
}

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
void Table< Key, Record, B, BlockSize, BlockAlignment >::initSnapshots()
{
    OTK_ASSERT( m_snapshotFile );

    char header[ 128 ];
    bool valid_header = true;
    size_t snapshotCount = 0;
    SimpleFile::offset_t curOffset = 0;

    try
    {
        m_snapshotFile->read( &header, 128, curOffset );
        curOffset += 128;
        std::string word( header, 9 );
        OTK_ASSERT( word.compare( "Snapshots" ) == 0 );

        snapshotCount = *( reinterpret_cast< size_t* >( header + 16 ) );
    }
    catch( ... )
    {
        /// New file.
        valid_header = false;
    }

    try
    {
        if( valid_header )
        {
            do
            {
                size_t snapHead[ 5 ]; // id, new count, modified count, total count, root.
                m_snapshotFile->read( &snapHead, 5 * sizeof( size_t ), curOffset );
                curOffset += 5 * sizeof( size_t );

                const size_t id = snapHead[ 0 ];
                const size_t nc = snapHead[ 1 ];
                const size_t mc = snapHead[ 2 ];
                const size_t tc = snapHead[ 3 ];
                const size_t rt = snapHead[ 4 ];

                OTK_ASSERT( nc + mc == tc );

                std::shared_ptr<Snapshot> snapshot( new Snapshot() );
                std::vector<size_t> in_data( tc, 0 );

                m_snapshotFile->read( in_data.data(), tc * sizeof( size_t ), curOffset );
                curOffset += tc * sizeof( size_t );

                snapshot->set_id( id );
                snapshot->set_root( rt );
                std::copy( in_data.begin(), in_data.begin() + nc,
                           std::inserter( snapshot->m_new_blocks, snapshot->m_new_blocks.end() ) );
                std::copy( in_data.begin() + nc, in_data.begin() + nc + mc,
                           std::inserter( snapshot->m_modified_blocks, snapshot->m_modified_blocks.end() ) );
                m_readers.insert( {snapshot, nullptr} );

                if( !m_latestSnapshot || *m_latestSnapshot < *snapshot )
                    m_latestSnapshot = snapshot;

                snapshotCount--;
            } while( snapshotCount );
        }
    }
    catch( ... )
    {
        // Invalid data. Do something?
    }
}

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
void Table< Key, Record, B, BlockSize, BlockAlignment >::writeSnapshots() const
{
    OTK_ASSERT( m_snapshotFile );
    OTK_ASSERT( m_snapshotFile->isWriteable() );

    char header[ 128 ] = "Snapshots";
    size_t snapshotCount = m_readers.size();
    SimpleFile::offset_t curOffset = 0;

    *( reinterpret_cast< size_t* >( header + 16 ) ) = snapshotCount;
    m_snapshotFile->write( &header, 128, curOffset );
    curOffset += 128;

    for( auto pair : m_readers )
    {
        std::shared_ptr<Snapshot> snap = pair.first;

        const size_t id = snap->m_snapshotId;
        const size_t nc = snap->m_newBlocks.size();
        const size_t mc = snap->m_modifiedBlocks.size();
        const size_t tc = nc + mc;
        const size_t rt = snap->m_root_index;

        size_t snapHead[ 5 ] = { id, nc, mc, tc, rt };// id, new count, modified count, total count, root.
        m_snapshotFile->write( &snapHead, 5 * sizeof( size_t ), curOffset );
        curOffset += 5 * sizeof( size_t );

        std::vector<size_t> blocks( snap->m_newBlocks.begin(), snap->m_newBlocks.end() );
        m_snapshotFile->write( blocks.data(), blocks.size() * sizeof( size_t ), curOffset );
        curOffset += nc * sizeof( size_t );

        blocks.assign( snap->m_modifiedBlocks.begin(), snap->m_modifiedBlocks.end() );
        m_snapshotFile->write( blocks.data(), blocks.size() * sizeof( size_t ), curOffset );
        curOffset += mc * sizeof( size_t );
    }
}

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
std::shared_ptr< TableWriter< Key, Record, B, BlockSize, BlockAlignment > > Table< Key, Record, B, BlockSize, BlockAlignment >::getWriter()
{
    std::unique_lock<std::mutex> lock( m_mutex );

    // Subsequent calls return the same writer.
    if( !m_writer )
    {
        // Create a new writer.
        m_writer.reset( new TableWriterType( m_dataFile, m_latestSnapshot ) );
    }

    return m_writer;
}

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
std::shared_ptr< TableReader< Key, Record, B, BlockSize, BlockAlignment > > Table< Key, Record, B, BlockSize, BlockAlignment >::getReader( std::shared_ptr< Snapshot > snapshot )
{
    std::unique_lock<std::mutex> lock( m_mutex );

    auto it = m_readers.find( snapshot );
    OTK_ASSERT( it != m_readers.end() );

    if( !it->second )
        it->second.reset( new TableReaderType() );

    return it->second;
}

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
void Table< Key, Record, B, BlockSize, BlockAlignment >::close()
{
    std::unique_lock<std::mutex> lock( m_mutex );
    m_writer.reset();
    m_readers.clear();    
}

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
void Table< Key, Record, B, BlockSize, BlockAlignment >::destroy()
{
    close();
    std::filesystem::remove_all( m_directory );
}

} // namespace sceneDB
