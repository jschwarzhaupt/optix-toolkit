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

// Forward declarations
template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
class TableWriter;

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
class TableReader;


// A Snapshot records information about which blocks
// in a Table are modified and/or new when compared
// to the immediately preceding Snapshot.
// This information can be used to determine if blocks
// can be freed / discarded without losing any data
// referenced by a given Snapshot.
// It also stores the index of the block that holds
// the root node for the current Snapshot of the Table.
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

    bool is_new( const size_t index ) const      { return m_new_blocks.count( index ) != 0; }
    bool is_modified( const size_t index ) const { return m_modified_blocks.count( index ) != 0; }

    size_t           m_snapshot_id{ 0 };
    size_t           m_root_index{ 0 };
    std::set<size_t> m_new_blocks;
    std::set<size_t> m_modified_blocks;
};

inline bool operator< ( const Snapshot& l, const Snapshot& r ) { return l.m_snapshot_id < r.m_snapshot_id; }


// A Link encodes information about where to find data and
// the kind of data pointed to.
// If a Link is a leaf, then it points to a Record.
// If a Link is local, then what it points to is in the same 
// TableDataBlock as the Node containing the Link.
// Default-constructed Links are invalid.
template <size_t BlockSize>
struct Link
{
    static constexpr size_t leaf_bit = size_t(1) << (sizeof(size_t) * 8 - 1);
    static constexpr size_t local_bit = leaf_bit >> 1;
    static constexpr size_t data_mask = ~(leaf_bit | local_bit);
    static constexpr size_t local_mask = make_mask(BlockSize);
    static constexpr size_t block_mask = data_mask & ~local_mask;
    static constexpr size_t block_shift = get_msb(BlockSize);
    static constexpr size_t block_limit = (block_mask >> block_shift) + 1;

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

    bool is_valid() const { return m_link != ~0ull; }
    void invalidate() { m_link = ~0ull; }

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

    // Returns the offset within the block at which the data resides.
    size_t get_local_address() const { return m_link & local_mask; }
    void set_local_address( size_t v )
    {
        OTK_ASSERT( v < BlockSize );
        m_link = ( m_link & ~local_mask ) | ( v & local_mask );
    }

    // Returns the index of the block at which the data resides.
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

    size_t m_link{ ~0ull };
};


// A Node is a sorted array of unique <Key, Link> pairs.
// Pairs with equivalent keys are considered equal.
template <typename Key, size_t B, size_t BlockSize>
struct Node
{
    Node()
    {
    }

    typedef struct Link<BlockSize> Link;
    typedef std::pair< Key, Link > Pair_T;

    // Leaf Nodes hold links that point to Records.
    bool is_leaf()         const { return m_metadata.is_leaf(); }
    void set_leaf( bool l )      { m_metadata.set_leaf( l ); }

    bool is_valid()        const { return m_valid_count > 0; }
    bool is_full()         const { return m_valid_count /* + 1*/ >= B; }
    uint16_t child_count() const { return m_valid_count; }

    // Reset the node so it has no children.
    void clear() { m_valid_count = 0; }
        
    // Returns whether the given link exists within this node.
    bool has_link( const Link& link ) const;

    // Search for link_old and, if found, replace it with link_new.
    void update_link( const Link& link_old, const Link& link_new );

    // Replace the block references from old_block to new_block in
    // all links within this node that point to old_block.
    void update_block( const size_t old_block, const size_t new_block );

    Pair_T& get_pair( uint16_t index )
    {
        OTK_ASSERT( index < m_valid_count );
        return m_pairs[ index ];
    }

    const Pair_T& get_pair( uint16_t index ) const
    {
        OTK_ASSERT( index < m_valid_count );
        return m_pairs[ index ];
    }

    // Returns the lowest Key stored in the Node.
    Key low_key() const
    {
        return m_pairs[ 0 ].first;
    }

    // Returns the highest Key stored in the Node.
    Key high_key() const
    {
        return m_pairs[ m_valid_count - 1 ].first;
    }

    // Search this node, which must be a leaf, for
    // a pair having key as it's Key.
    // If such a pair is found, copy it to o_pair.
    // Returns true if found, false otherwise.
    bool find( const Key& key, Pair_T* o_pair ) const;

    // Search this node for the pair having the
    // largest Key that is smaller or equal to the
    // given key.
    // If such pair exists, returns the link held by it.
    // If not, returns the link from the pair holding the smallest Key.
    // If update_low_key is true, then update the key for that pair.
    Link traverse( const Key& key, bool update_low_key );
    Link traverse( const Key& key ) const;

    // Insert a <Key, Link> pair into this node.
    // If a pair with a matching Key exists,
    // then it's link is replaced with the one from the given pair.
    // If not, then this node must not be full.
    // The new pair is inserted while keeping m_pairs in sorted Key order.
    bool insert( const Pair_T& pair );

    // Find a pair in m_pairs with Key matching the one from the 
    // given pair. If found, erase it and return true.
    // Otherwise, return false.
    bool erase( const Pair_T& pair );

    // Erases the pair m_pair[ index ].
    // index must be smaller than child_count()
    void erase( uint16_t index );

    Pair_T   m_pairs[ B ];
    uint16_t m_valid_count{ 0 };
    Link     m_metadata;
};


// A Table is a key-value store or dictionary that supports snapshots.
// It is internally implemented as a B-Tree that is stored within a BlockFile,
// while metadata supporting the snapshot mechanism is stored within a SimpleFile.
// Keys must be PODs and comparable via the less-than and the equals operators,
// such that !(key1 < key2) && !(key2 < key1) implies that key1 == key2 (ie, they are equivalent).
// Records must be PODs.
// The branching factor of the B-Tree is given by the 'B' template parameter.
// 'BlockSize' and 'BlockAlignment' specify the parameters for the BlockFile.
// The Table object itself provides limited functionality. In order to read from a Table,
// a user needs to obtain a TableReader object by calling the getReader() method.
// In order to write to a table, users need to obtain a TableWriter object by calling
// the getWriter() method.
template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
class Table
{
    static constexpr size_t key_size = sizeof( Key );
    static constexpr size_t record_size = sizeof( Record );
    static constexpr size_t record_alignment = alignof( Record );

public:
    typedef TableWriter<Key, Record, B, BlockSize, BlockAlignment> TableWriterType;
    typedef TableReader<Key, Record, B, BlockSize, BlockAlignment> TableReaderType;
    typedef struct Node<Key, B, BlockSize> Node;

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
    // The Header stores information that is used to validate
    // that a Table stored within a BlockFile is compatible
    // with a specific instantiation of a Table.
    // Specifically, it can be verified that the size of the
    // keys and records stored within the file matches the
    // parameters of a given Table object.
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


// TableWriterDataBlock is a wrapper class around a BlockFile::DataBlock.
// It provides the high-level functionality needed to store the Table.
// In particular, blocks store metadata, Nodes and Records.
// Nodes are allocated within a block starting at its lowest address,
// with subsequent Nodes allocated at increasingly higher addresses.
// The metadata is stored at the very end of the block.
// Records are allocated starting at the highest address prior to the
// metadata section, and their offsets decrease for subsequent Records.
template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
class TableWriterDataBlock
{
public:
    typedef Table< Key, Record, B, BlockSize, BlockAlignment > TableType;
    typedef typename TableType::Node         Node;
    typedef typename TableType::Node::Link   Link;
    typedef typename TableType::Node::Pair_T Pair_T;

    // Runtime metadata required to keep track of allocated
    // and erased elements within the block.
    // This metadata is not stored with the blocks, but is
    // rebuilt when a block is loaded from disk.
    struct Metadata
    {
        int m_next_node{ 0 };
        int m_next_record{ 0 };
        int m_record_count{ 0 };
        std::set< int > m_free_nodes;
        std::set< int > m_free_records;
    };

    // Blocks can hold up to B different subtrees. When splitting
    // a Block, we must know each subtree root.
    // RootData stores this information.
    // RootData is stored within the block itself.
    struct RootData
    {
        void add_root( uint32_t offset ) { OTK_ASSERT( m_root_count < B ); m_root_offsets[ m_root_count++ ] = offset; }
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

    TableWriterDataBlock( std::shared_ptr< DataBlock > data )
        : m_dataBlock( data )
        , m_index( data->index )
    {
        OTK_ASSERT( m_dataBlock->size == BlockSize );
        OTK_ASSERT( usable_size() >= ( B * sizeof( Record ) + sizeof( Node ) ) );
    }

    // Initialize a new block.
    // Set the snapshot ID to the given snapshot's.
    // Add a new Node and mark it as a root.
    void init( const Snapshot& snapshot );

    // Initialize the metadata when reading
    // a previously written DataBlock.
    void init_existing();

    // Reset the pointer to the DataBlock so that it can be freed from memory.
    // This avoids destroying the Metadata, which would otherwise need to be
    // recreated if the DataBlock is then loaded again.
    void release() { m_dataBlock.reset(); m_valid = false; }

    // A TableDataBlock is valid if it's been initialized and holds a non-null DataBlock.
    bool is_valid() const { return m_valid && bool( m_dataBlock.get() ); }

    // Set the pointer to the DataBlock so it points to 'data'.
    // The DataBlock must have the same index (ie, be the same)
    // as the one used to contruct this TableDataBlock.
    void set_data( std::shared_ptr< DataBlock > data )
    {
        OTK_ASSERT( data->size == BlockSize );
        OTK_ASSERT( data->index == m_index );
        m_dataBlock = data;
    }

    // Reclaim any space that has been left by erasing Nodes.
    // This space is tracked by keeping a list of free Nodes.
    // As this operation may move Nodes around, we need a pointer to
    // the parent Node that holds links pointing into this block,
    // so that those links can be updated.
    // Parent may not exist if this is the root block.
    void compact_nodes( Node* parent );

    // Reclaim any space that has been left by erasing Records.
    // This space is tracked by keeping a list of free Records.
    // This operation doesn't move Nodes around, so only Nodes
    // that are local to the block may need to have links updated.
    void compact_records();
        
    // Returns the number of bytes to be gained by compacting Records.
    size_t compact_records_free_bytes() const { return m_metaData.m_free_records.size() * sizeof( Record ); }

    // Returns the number of bytes to be gained by compacting Nodes.
    size_t compact_nodes_free_bytes() const { return m_metaData.m_free_nodes.size() * sizeof( Node ); }

    // Returns the number of bytes available without doing any compaction.
    size_t no_compact_free_bytes() const { return m_metaData.m_next_record + sizeof( Record ) - m_metaData.m_next_node; }

    // Returns the total number of bytes available if all free space is compacted.
    size_t total_free_bytes() const { return compact_nodes_free_bytes() + compact_records_free_bytes() + no_compact_free_bytes(); }

    // Returns whether the block has enough space to store n values of size bytes each.
    // 'need_node_compaction' flags if compacting Nodes is necessary to have enough space.
    // 'need_record_compaction' flags if compacting Records is necessary to have enough space.
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

    bool has_room_for_n_nodes( uint32_t n, 
                               bool&    need_node_compaction, 
                               bool&    need_record_compaction ) const
    {
        return has_room_for_n< sizeof(Node) >( n, need_node_compaction, need_record_compaction );
    }

    bool has_room_for_n_records( uint32_t   n, 
                                 bool&      need_node_compaction, 
                                 bool&      need_record_compaction ) const
    {
        return has_room_for_n< sizeof(Record) >( n, need_node_compaction, need_record_compaction );
    }

    size_t usable_size() const { return root_data_offset(); }

    // Returns whether this block is full or not.
    // A block is considered full if, for blocks not holding
    // any Records, there is not enough space to store an
    // additional Node.
    // For blocks holding Records, it is considered full if
    // there is not enough space to store the larger of an
    // additional Node or an additional Record.
    bool is_full() const { return total_free_bytes() < ( m_metaData.m_record_count ? std::max( sizeof( Node ), sizeof( Record ) ) : sizeof( Node ) ); }

    // Allocate a new Node in the block.
    // Returns the offset to the new Node if successfull, uint32_t( -1 ) otherwise.
    uint32_t add_node();

    // Remove the Node at offset and add its space to the free list.
    void remove_node( const uint32_t offset );

    Node* node( const size_t offset )             { OTK_ASSERT( validate_node_offset( offset ) ); return node_ptr( offset ); }
    const Node* node( const size_t offset ) const { OTK_ASSERT( validate_node_offset( offset ) ); return node_ptr( offset ); }

    // Allocate a new Record in the block.
    // Returns the offset to the new Record if successfull, uint32_t( -1 ) otherwise.
    uint32_t add_record( const Record& new_record );

    // Remove the Record at offset and add its space to the free list.
    void remove_record( const uint32_t offset );

    Record& record( const size_t offset )             { OTK_ASSERT( validate_record_offset( offset ) ); return *record_ptr( offset ); }
    const Record& record( const size_t offset ) const { OTK_ASSERT( validate_record_offset( offset ) ); return *record_ptr( offset ); }

    // Record that the Node at offset is a root of the block.
    void add_root( const uint32_t offset );

    // Record that the Node at offset is NOT a root of the block.
    void remove_root( const uint32_t offset );

    // Return the number of root Nodes in this block.
    uint32_t root_count() const { return root_data_ptr()->m_root_count; }

    // Return the offset to the n'th root Node.
    size_t root_offset( uint32_t n ) const { OTK_ASSERT( n < root_count() ); return root_data_ptr()->m_root_offsets[ n ]; }

    // Check if the Node at offset is a root.
    bool is_root( uint32_t offset ) const { return root_data_ptr()->is_root( offset ); }

    // Move the portion of the subtree, with root Node at src_root_offset, that is stored
    // within this block, to dst_block.
    // Returns the offset within dst_block that holds the new root Node.
    uint32_t migrate_subtree( uint32_t src_root_offset, std::shared_ptr<TableWriterDataBlock> dst_block );

    // Set the snapshot ID for this block.
    void set_snapshot( const Snapshot& snapshot ) { snapshot_id() = snapshot.m_snapshot_id; }

    size_t index() const { return m_index; }

private:
    static constexpr uint32_t snap_id_offset()   { return BlockSize - sizeof( std::size_t ); }
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

    bool validate_node_offset( const size_t offset ) const   { return ( offset <= m_metaData.m_next_node - sizeof(Node) ) && ( offset % sizeof(Node) == 0 ); }
    bool validate_record_offset( const size_t offset ) const { return ( offset >= m_metaData.m_next_record + sizeof(Record) ) && ( offset <= record_offset() ) && ( offset % alignof(Record) == 0 ); }

    size_t& snapshot_id() { return *snap_id_ptr(); }

    std::shared_ptr< DataBlock > m_dataBlock;
    Metadata                     m_metaData;
    size_t                       m_index;
    bool                         m_valid{ false };
};


// A TableWriter is a helper class that can modify Tables.
// It can currently insert new <Key, Record> pairs and
// create Snapshots of the Table.
// TableWriters keep runtime data about blocks that have
// been loaded from disk, such that they can be loaded and
// unloaded from memory on demand.
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
  typedef struct TableWriterDataBlock<Key, Record, B, BlockSize, BlockAlignment> TableWriterDataBlock;
  typedef std::shared_ptr<TableWriterDataBlock> TableWriterDataBlockPtr;

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
            // This is a new Table and there are no prior snapshots.
            // Start from snapshot zero.
            m_currentSnapshot.set_id( 0 );
            m_currentSnapshot.set_root( 0 );
        }
    }

    // Insert the <Key, Record> pair into the Table.
    // This method is not thread-safe.
    void Insert( const Key& key, const Record& record );

    // Create a Snapshot recording the state of the Table
    // at the point in time when the Snapshot is created.
    Snapshot&& TakeSnaphot();

    // No copying
    TableWriter( const TableWriter& o ) = delete;
    
private:
    // Split a block into two or more blocks.
    // For blocks with a single root, create one new block for each subtree
    // starting at that root.
    // For a block with N roots, create N-1 new blocks. Each block gets one
    // of the trees, and one remains in the current block.
    void split_block( TableWriterDataBlockPtr block, Node* block_parent );

    // Returns whether the split succeeded or not.
    // Split may not succeed if the block needs to be split,
    // in which case traversal needs to be restarted from the
    // block's parent (as nodes can move).
    bool split_node(TableWriterDataBlockPtr block, Node* parent, Node* block_parent, Link src_link);

    // Copies the block at offset block_index into a new block.
    // Returns the offset for the new block.
    size_t copy_on_write( const size_t block_index );

    // Fetch the block at the given offset.
    // If the block has already been loaded, this returns immediately.
    // Otherwise, the block is loaded from disk.
    // Returns a pointer to the block.
    TableWriterDataBlockPtr get_block( size_t index );

    // Allocate a new block in the BlockFile and initializes it to the current snapshot.
    // Returns a pointer to the block.
    TableWriterDataBlockPtr get_new_block();

    // Returns a pointer to the block containing the root of the Table 
    // for the current snapshot.
    TableWriterDataBlockPtr get_root_block();

    // Returns a Link referencing the root block of the Table.
    Link get_root_link();

    Link                                        m_root_link;
    Snapshot                                    m_currentSnapshot;
    std::map< size_t, TableWriterDataBlockPtr > m_blockMap;
    std::shared_ptr< BlockFile >                m_dataFile;
};

/* Node method definitions */

template <typename Key, size_t B, size_t BlockSize>
bool Node<Key, B, BlockSize>::has_link( const Link& link ) const
{
    for( uint16_t i = 0; i < m_valid_count; ++i )
        if( m_pairs[i].second == link )
            return true;
    return false;
}

template <typename Key, size_t B, size_t BlockSize>
void Node<Key, B, BlockSize>::update_link( const Link& link_old, const Link& link_new )
{
    for( uint16_t i = 0; i < m_valid_count; ++i )
        if( m_pairs[i].second == link_old )
        {
            m_pairs[i].second = link_new;
        }
}

template <typename Key, size_t B, size_t BlockSize>
void Node<Key, B, BlockSize>::update_block(const size_t old_block, const size_t new_block)
{
    for (uint16_t i = 0; i < m_valid_count; ++i)
        if (m_pairs[i].second.get_block() == old_block)
            m_pairs[i].second.set_block(new_block);
}

template <typename Key, size_t B, size_t BlockSize>
bool Node<Key, B, BlockSize>::find( const Key& key, Pair_T* o_pair ) const
{
    OTK_ASSERT( o_pair );
    OTK_ASSERT( is_leaf() );
    const Pair_T* start = m_pairs, end = start + m_valid_count;
    const Pair_T* it = std::lower_bound( start, end, key,
        []( const Pair_T& e, const Key& c ) {
            return e.first < c;
        });
    if( it == end ||
        it->first != key )
        return false;

    *o_pair = *it;
    return true;
}

template <typename Key, size_t B, size_t BlockSize>
typename Node<Key, B, BlockSize>::Link Node<Key, B, BlockSize>::traverse( const Key& key ) const
{
    if( key <= low_key() )
        return m_pairs[ 0 ].second;
    if( key >= high_key() )
        return m_pairs[ m_valid_count - 1 ].second;

    const Pair_T* start = m_pairs + 1, * end = start + m_valid_count;
    const Pair_T* it = std::lower_bound( start, end, key,
        []( const Pair_T& e, const Key& c ) {
            return e.first < c;
        });
    return (it - 1)->second;
}

template <typename Key, size_t B, size_t BlockSize>
typename Node<Key, B, BlockSize>::Link Node<Key, B, BlockSize>::traverse( const Key& key, bool update_low_key )
{
    if( update_low_key &&
        key <= low_key() )
        m_pairs[ 0 ].first = key;
    return traverse( key );
}

template <typename Key, size_t B, size_t BlockSize>
bool Node<Key, B, BlockSize>::insert( const Pair_T& pair )
{
    Pair_T* start = m_pairs, * end = start + m_valid_count;
    Pair_T* it = std::lower_bound( start, end, pair.first,
        []( const Pair_T& e, const Key& c ) {
            return e.first < c;
        });
    bool exists = ( it != end &&
                    it->first == pair.first );

    OTK_ASSERT( !is_full() || exists );

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

template <typename Key, size_t B, size_t BlockSize>
bool Node<Key, B, BlockSize>::erase( const Pair_T& pair )
{
    Pair_T* start = m_pairs, * end = start + m_valid_count;
    Pair_T* it = std::lower_bound( start, end, pair.first,
        []( const Pair_T& e, const Key& c ) {
            return e.first < c;
        });
    bool exists = ( it != end &&
                    it->first == pair.first );

    if( !exists )
        return false;

    for( Pair_T* src = it; src < end; ++src )
        *src = *(src + 1);
    m_valid_count--;

    return true;
}

template <typename Key, size_t B, size_t BlockSize>
void Node<Key, B, BlockSize>::erase( uint16_t index )
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


/* Table method definitions */


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

        const size_t id = snap->m_snapshot_id;
        const size_t nc = snap->m_new_blocks.size();
        const size_t mc = snap->m_modified_blocks.size();
        const size_t tc = nc + mc;
        const size_t rt = snap->m_root_index;

        size_t snapHead[ 5 ] = { id, nc, mc, tc, rt };// id, new count, modified count, total count, root.
        m_snapshotFile->write( &snapHead, 5 * sizeof( size_t ), curOffset );
        curOffset += 5 * sizeof( size_t );

        std::vector<size_t> blocks( snap->m_new_blocks.begin(), snap->m_new_blocks.end() );
        m_snapshotFile->write( blocks.data(), blocks.size() * sizeof( size_t ), curOffset );
        curOffset += nc * sizeof( size_t );

        blocks.assign( snap->m_modified_blocks.begin(), snap->m_modified_blocks.end() );
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
    m_dataFile->close();
    m_snapshotFile->close();
}

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
void Table< Key, Record, B, BlockSize, BlockAlignment >::destroy()
{
    close();
    m_dataFile->destroy();
    m_snapshotFile->destroy();
}


/* TableWriterDataBlock method definitions */


template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
void TableWriterDataBlock<Key, Record, B, BlockSize, BlockAlignment>::compact_nodes( Node* parent )
{
    RootData& rd = *root_data_ptr();
    std::map< int, int > remap;

    while( !m_metaData.m_free_nodes.empty() )
    {
        int offset_new = m_metaData.m_free_nodes.extract( m_metaData.m_free_nodes.begin() ).value();
        int offset_old = m_metaData.m_next_node - sizeof( Node );

        // Move the Node at offset_old into the space at offset_new.
        *node( offset_new ) = *node( offset_old );

        // If the Node is a root, we must update the link to it in parent.
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

        // For interior Nodes, we will need to update any links that
        // point to them. Keep track of what we need to change.
        if( !is_root )
            remap.insert( { offset_old, offset_new } );

        // Shift back the offset to the next available Node.
        m_metaData.m_next_node -= sizeof( Node );
        while( m_metaData.m_free_nodes.count( m_metaData.m_next_node - sizeof( Node ) ) )
        {
            // Advance through contiguous free space.
            m_metaData.m_next_node -= sizeof( Node );
            m_metaData.m_free_nodes.erase( m_metaData.m_free_nodes.find( m_metaData.m_next_node ) );
        }
    }

    // If remap is empty, only root Nodes (or no Nodes) were moved.
    // For root Nodes, we already updated the links to them. Done.
    if( remap.empty() )
        return;

    // Now iterate through all valid Nodes, updating links as we go.
    for( int offset = 0; offset < m_metaData.m_next_node && !remap.empty(); offset += sizeof( Node ) )
    {
        for( auto it = remap.begin(); it != remap.end(); ++it )
        {
            Link link_old( true, true, index(), it->first );

            if( node( offset )->has_link( link_old ) )
            {
                Link link_new( true, true, index(), it->second );   
                node( offset )->update_link( link_old, link_new );
                it = remap.erase( it );
            }
        }
    }
}

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
void TableWriterDataBlock<Key, Record, B, BlockSize, BlockAlignment>::compact_records()
{
    std::map< int, int > remap;

    while( !m_metaData.m_free_records.empty() )
    {
        int offset_new = m_metaData.m_free_records.extract( std::prev( m_metaData.m_free_records.end() ) ).value();
        int offset_old = m_metaData.m_next_record + sizeof( Record );

        // Move the record at offset_old into the space at offset_new.
        record( offset_new ) = record( offset_old );

        //Keep track of what we need to change.
        remap.insert( { offset_old, offset_new } );

        // Shift forward the offset to the next available Record.
        m_metaData.m_next_record += sizeof( Record );
        while( m_metaData.m_free_records.count( m_metaData.m_next_record + sizeof( Record ) ) )
        {
            // Advance through contiguous free space.
            m_metaData.m_next_record += sizeof( Record );
            m_metaData.m_free_records.erase( m_metaData.m_free_records.find( m_metaData.m_next_record ) );
        }
    }

    // If remap is empty, nothing was moved. Done.
    if( remap.empty() )
        return;

    // Now iterate through all valid Nodes, updating links as we go.
    for( int offset = 0; offset < m_metaData.m_next_node && !remap.empty(); offset += sizeof( Node ) )
    {
        // Skip free and non-leaf Nodes.
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
                it = remap.erase( it );
            }
        }
    }
}

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
uint32_t TableWriterDataBlock<Key, Record, B, BlockSize, BlockAlignment>::add_node()
{
    int result = -1;

    // Try to first use previously removed Nodes, if possible.
    if (m_metaData.m_free_nodes.size())
    {
        result = m_metaData.m_free_nodes.extract(m_metaData.m_free_nodes.begin()).value();
    }
    else if (no_compact_free_bytes() >= sizeof(Node))
    {
        result = m_metaData.m_next_node;
        m_metaData.m_next_node += sizeof(Node);
    }

    if( result >= 0 )
    {
        new (node(result)) Node();
    }

    return uint32_t(result);
}

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
void TableWriterDataBlock<Key, Record, B, BlockSize, BlockAlignment>::remove_node( const uint32_t offset )
{
    OTK_ASSERT( validate_node_offset( offset ) );
    OTK_ASSERT( offset < usable_size() );
    if( offset >= m_metaData.m_next_node )
        return;

    if( is_root( offset ) )
        remove_root( offset );

    // Easy case - removing the last Node. Instead of adding
    // to the free list, shift back the offset to the next available
    // Node to reclaim the free space immediately.
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

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
uint32_t TableWriterDataBlock<Key, Record, B, BlockSize, BlockAlignment>::add_record( const Record& new_record )
{
    int result = -1;

    // Try to first use previously removed Nodes, if possible.
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

        // Keep track of how many records the block has stored.
        m_metaData.m_record_count++;
    }

    return uint32_t( result );
}

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
void TableWriterDataBlock<Key, Record, B, BlockSize, BlockAlignment>::remove_record( const uint32_t offset )
{
    OTK_ASSERT( validate_record_offset( offset ) );
    OTK_ASSERT( offset < usable_size() );
    if( offset <= m_metaData.m_next_record )
        return;

    // Keep track of how many records the block has stored.
    m_metaData.m_record_count--;

    // Easy case - removing the last Record. Instead of adding
    // to the free list, shift forward the offset to the next available
    // Record to reclaim the free space immediately.
    if( m_metaData.m_next_record + sizeof( Record ) == offset )
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

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
void TableWriterDataBlock<Key, Record, B, BlockSize, BlockAlignment>::add_root( const uint32_t offset )
{
    OTK_ASSERT( validate_node_offset( offset ) );
    auto rd = root_data_ptr();
    OTK_ASSERT( rd->m_root_count < B );
    rd->m_root_offsets[ rd->m_root_count++ ] = offset;
}

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
void TableWriterDataBlock<Key, Record, B, BlockSize, BlockAlignment>::remove_root( const uint32_t offset )
{
    OTK_ASSERT( validate_node_offset( offset ) );
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

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
uint32_t TableWriterDataBlock<Key, Record, B, BlockSize, BlockAlignment>::migrate_subtree( uint32_t src_root_offset, std::shared_ptr< TableWriterDataBlock > dst_block )
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

    // Traverse the tree, depth-first, migrating Nodes
    // and Records to dst_block.
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
                    // Migrate Record to dst_block.
                    OTK_ASSERT( src_pair.second.is_leaf() );
                    uint32_t offset = dst_block->add_record( record( src_pair.second.get_local_address() ) );
                    OTK_ASSERT( offset != uint32_t( -1 ) );
                    dst_pair.second.set_local_address( offset );
                    // Now remove the stale Record from this block.
                    remove_record( src_pair.second.get_local_address() );
                }
                else
                {
                    // Migrate Node to dst_block.
                    OTK_ASSERT( !src_pair.second.is_leaf() );
                    uint32_t offset = dst_block->add_node();
                    OTK_ASSERT( offset != uint32_t( -1 ) );
                    dst_pair.second.set_local_address( offset );
                    // Push to the stack.
                    s.push( { uint32_t( src_pair.second.get_local_address() ), offset } );
                }
            }

            // Insert the new pair to the new Node.
            dst_node->insert( dst_pair );
        }

        // Remove the stale Node from this block.
        remove_node( offset_pair.first );

    } while (!s.empty());

    return dst_root_offset;
}

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
void TableWriterDataBlock<Key, Record, B, BlockSize, BlockAlignment>::init( const Snapshot& snapshot )
{
    set_snapshot( snapshot );
    new ( root_data_ptr() ) RootData();
    m_metaData.m_next_record = record_offset();

    auto offset = add_node();
    OTK_ASSERT( offset == 0 );
    add_root( offset );
    m_valid = true;
}

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
void TableWriterDataBlock<Key, Record, B, BlockSize, BlockAlignment>::init_existing()
{
    const auto& root_data = *root_data_ptr();
            
    // Keep track of valid Nodes and Records.
    std::set< uint32_t > nodes;
    std::set< uint32_t > records;

    // Traverse each subtree in the block to rebuild
    // its structure.
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

                // Sanity check
                OTK_ASSERT( !link.is_local() || link.get_block() == index() );
            }
        } while( !s.empty() );
    }

    OTK_ASSERT( nodes.size() );
    m_metaData.m_next_node = *nodes.rbegin() + sizeof( Node );
    if( records.size() )
        m_metaData.m_next_record = *records.begin() - sizeof( Record );
    else
        m_metaData.m_next_record = record_offset();

    // Now, add any holes in the set of valid Nodes
    // as free Nodes.
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

    // Add any holes in the set of valid Records
    // as free Records.
    if( records.size() )
    {
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

    m_valid = true;
}


/* TableWriter method definitions */


template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
void TableWriter<Key, Record, B, BlockSize, BlockAlignment>::Insert( const Key& key, const Record& record )
{
    // Non-concurrent version.         
    Link root_link         = get_root_link();
    Link block_parent_link = Link();
    Link parent_link       = Link();
    Link current_link      = root_link;

    TableWriterDataBlockPtr block = get_root_block();

    while( true )
    {
        // Check whether the block is full. If it is,
        // we must split it before proceeding.
        if( block->is_full() )
        {
            Node* block_parent_node = nullptr;
            if( block_parent_link.is_valid() )
            {
                // There is a valid link to a parent Node.
                // We'll need to update it while splitting.
                block_parent_node = get_block( block_parent_link.get_block() )->node( block_parent_link.get_local_address() );
            }

            split_block( block, block_parent_node );

            // Splitting the block moved things around, so we must
            // re-start traversal from the last known-good Node, which
            // would be the block_parent.
            // If there's no block_parent, we just re-start from the root.
            parent_link = block_parent_link;
            if( block_parent_node )
                current_link = block_parent_node->traverse( key );
            else
                current_link = root_link;

            block = get_block( current_link.get_block() );
            continue;
        }

        Node* current_node = block->node( current_link.get_local_address() );
        // Now check whether the current Node is full.
        // If so, we must also split it before proceeding.
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

            // Splitting a Node can, potentially, fail. This can happen
            // if we had to split the block or perform Node compaction, which
            // may move Nodes around.
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

            // After successfully splitting the node, the Key to be inserted
            // may belong in the newly allocated Node, so we re-traverse the
            // parent Node.
            if( parent_node )
            {
                current_link = parent_node->traverse( key );
                block = get_block( current_link.get_block() );
                current_node = block->node( current_link.get_local_address() );
            }
            else
            {
                // Can only happen for the Table's root Node.
                // Things shouldn't have changed.
                OTK_ASSERT( current_link == root_link );
                OTK_ASSERT( block->index() == current_link.get_block() );
                OTK_ASSERT( block->node( current_link.get_local_address() ) == current_node );
            }
        }

        // Found a leaf, insert here.
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

        // Traverse through the current Node, potentially
        // moving to a different block.
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

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
void TableWriter<Key, Record, B, BlockSize, BlockAlignment>::split_block( TableWriterDataBlockPtr block, Node* block_parent )
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

        TableWriterDataBlockPtr new_block = get_new_block();
        size_t offset = block->migrate_subtree( child_pair.second.get_local_address(), new_block );

        child_pair.second.set_local( false );
        child_pair.second.set_block( new_block->index() );
        child_pair.second.set_local_address( offset );
    }
}

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
bool TableWriter<Key, Record, B, BlockSize, BlockAlignment>::split_node( TableWriterDataBlockPtr block, Node* parent, Node* block_parent, Link src_link )
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

            TableWriterDataBlockPtr new_block = get_new_block();

            uint16_t start = 0;
            uint16_t end   = left_count;
            if( left_n > right_n )
            {
                start = left_count;
                end   = count;
            }

            TableWriterDataBlockPtr src_block = get_block( block_index );
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

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
size_t TableWriter<Key, Record, B, BlockSize, BlockAlignment>::copy_on_write( const size_t block_index )
{
    auto old_block = m_dataFile->checkOutForRead( block_index );
    auto new_block = m_dataFile->checkOutNewBlock();

    *new_block = *old_block;
    TableWriterDataBlockPtr block = std::make_shared< TableWriterDataBlock >( new_block );
    block->set_snapshot( m_currentSnapshot );
    block->init_existing();
    block->compact_records(); // Can't compact nodes as we would need to know the parent.

    m_currentSnapshot.m_modified_blocks.insert( old_block->index );
    m_currentSnapshot.m_new_blocks.insert( new_block->index );

    m_blockMap[ new_block->index ] = block;

    return new_block->index;
}

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
typename TableWriter<Key, Record, B, BlockSize, BlockAlignment>::TableWriterDataBlockPtr TableWriter<Key, Record, B, BlockSize, BlockAlignment>::get_block( size_t index )
{
    OTK_ASSERT( m_dataFile->exists( index ) );
    if( !m_currentSnapshot.is_new( index ) )
    {
        index = copy_on_write( index );
    }
    else if( !m_blockMap.count( index ) )
    {
        m_blockMap.emplace( std::make_pair( index, std::make_shared< TableWriterDataBlock >( m_dataFile->checkOutForReadWrite( index ) ) ) );
        m_blockMap[ index ]->init_existing();
    }

    return m_blockMap.at( index );
}

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
typename TableWriter<Key, Record, B, BlockSize, BlockAlignment>::TableWriterDataBlockPtr TableWriter<Key, Record, B, BlockSize, BlockAlignment>::get_new_block()
{
    TableWriterDataBlockPtr new_block = std::make_shared< TableWriterDataBlock >( m_dataFile->checkOutNewBlock() );
    new_block->init( m_currentSnapshot );
    m_currentSnapshot.m_new_blocks.insert( new_block->index() );
    m_blockMap[ new_block->index() ] = new_block;
    return new_block;
}

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
typename TableWriter<Key, Record, B, BlockSize, BlockAlignment>::TableWriterDataBlockPtr TableWriter<Key, Record, B, BlockSize, BlockAlignment>::get_root_block()
{
    if( !m_dataFile->exists( m_currentSnapshot.m_root_index ) )
    {
        TableWriterDataBlockPtr new_block = get_new_block();
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
        m_blockMap.emplace( m_currentSnapshot.m_root_index, std::make_shared< TableWriterDataBlock >( m_dataFile->checkOutForReadWrite( m_currentSnapshot.m_root_index ) ) );
        m_blockMap[ m_currentSnapshot.m_root_index ]->init_existing();
    }

    return m_blockMap.at( m_currentSnapshot.m_root_index );
}

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
typename TableWriter<Key, Record, B, BlockSize, BlockAlignment>::Link TableWriter<Key, Record, B, BlockSize, BlockAlignment>::get_root_link()
{
    if( !m_root_link.is_valid() )
    {
        TableWriterDataBlockPtr root_block = get_root_block();

        m_root_link.set_local( false );
        m_root_link.set_leaf( false );
        m_root_link.set_block( m_currentSnapshot.m_root_index );
        m_root_link.set_local_address( root_block->root_offset( 0 ) );
    }

    return m_root_link;
}

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
Snapshot&& TableWriter<Key, Record, B, BlockSize, BlockAlignment>::TakeSnaphot()
{
    m_dataFile->flush( false );

    Snapshot snapshot( std::move( m_currentSnapshot ) );

    m_currentSnapshot.set_id( snapshot.m_snapshot_id + 1 );
    m_currentSnapshot.set_root( snapshot.m_root_index );

    m_root_link.invalidate();

    return std::move( snapshot );
}

} // namespace sceneDB
