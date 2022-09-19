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
        : m_snapshotId( o.m_snapshotId )
        , m_rootIndex( o.m_rootIndex )
        , m_newBlocks( std::move( o.m_newBlocks ) )
        , m_modifiedBlocks( std::move( o.m_modifiedBlocks ) )
    {
        o.m_snapshotId = ~0ull;
        o.m_rootIndex = ~0ull;
    }

    void setId( const size_t id ) { m_snapshotId = id; }
    void setRoot( const size_t root ) { m_rootIndex = root; }

    bool is_new( const size_t index ) { return m_newBlocks.count( index ) != 0; }
    bool is_modified( const size_t index ) { return m_modifiedBlocks.count( index ) != 0; }

    size_t           m_snapshotId;
    size_t           m_rootIndex;
    std::set<size_t> m_newBlocks;
    std::set<size_t> m_modifiedBlocks;
};

bool operator< ( const Snapshot& l, const Snapshot& r ) { return l.m_snapshotId < r.m_snapshotId; }

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
            Link( bool isLeaf, bool isLocal, size_t block, size_t local_address )
            {
                OTK_ASSERT( !isLeaf || isLocal ); // Leaf links must be local
                setLeaf( isLeaf );
                setLocal( isLocal );
                setBlock( block );
                setLocalAddress( local_address );
            }
            Link( const Link& o ) : m_link( o.m_link ) {}

            Link& operator=( const Link& o )
            {
                m_link = o.m_link;
                return *this;
            }

            bool isValid() const { return !( m_link ^ size_t( -1 ) ); }

            bool isLeaf() const { return m_link & leaf_bit; }
            void setLeaf( bool l )
            {
                if( l )
                    m_link |= leaf_bit;
                else
                    m_link &= ~leaf_bit;
            }

            bool isLocal() const { return m_link & local_bit; }
            void setLocal( bool l )
            {
                if( l )
                    m_link |= local_bit;
                else
                    m_link &= ~local_bit;
            }

            size_t getLocalAddress() const { return m_link & local_mask; }
            void setLocalAddress( size_t v )
            {
                OTK_ASSERT( v < BlockSize );
                m_link = ( m_link & ~local_mask ) | ( v & local_mask );
            }

            size_t getBlock() const { return ( m_link & block_mask ) >> block_shift; }
            void setBlock( size_t b )
            {
                OTK_ASSERT( b < block_limit );
                m_link = ( m_link & ~block_mask ) | ( b << block_shift );
            }

            size_t m_link{ static_cast<size_t>( -1 ) };
        };

        Node()
        {
            side_link().first = Key( 0 );
        }

        typedef std::pair< Key, Link > Pair_T;

        Pair_T& side_link() { return m_pairs[ B - 1 ]; }

        bool isLeaf()  const { return side_link().second.isLeaf(); }
        bool isValid() const { return side_link().second.isValid(); }
        bool isFull()  const { return m_validCount + 1 == B; }

        Pair_T& find( const Key& key )
        {
            Pair_T* it = std::lower_bound( m_pairs, m_pairs + m_validCount, key,
                                           []( const Pair_T& e, const Key& c ) {
                                               return e.first < c;
                                           } );
            if( it - m_pairs )
                return *( it - 1 );
            return *it;
        }

        bool tryLatch()
        {
            return m_latch.compare_exchange_weak( false, true );
        }

        void unlatch()
        {
            m_latch.store( false );
        }

        Pair_T   m_pairs[ B ];
        uint16_t m_validCount{ 0 };
        std::atomic<bool> m_latch{ false };
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
bool operator==( const Table<Key, Record, B, BlockSize, BlockAlignment>::Node::Link& a, const Table<Key, Record, B, BlockSize, BlockAlignment>::Node::Link& b )
{
    return a.m_link == b.m_link;
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
        TableDataBlock( std::shared_ptr< DataBlock > data )
            : m_dataBlock( data )
        {
        }

        void init( const Snapshot& snapshot )
        {
            set_snapshot( snapshot );
            new ( ptr() ) Node();

            next_node() = sizeof( Node );
            next_record() = snap_id_offset() - record_size;
        }

        void set_snapshot( const Snapshot& snapshot )
        {
            snapshot_id() = snapshot.m_snapshotId;
        }

        Node* root() { return node_ptr( 0 ); }
        const Node* root() const { return node_ptr( 0 ); }

        Node* node( const size_t offset ) { OTK_ASSERT( offset < next_node() ); return node_ptr( offset ); }
        const Node* node( const size_t offset ) const { OTK_ASSERT( offset < next_node() ); return node_ptr( offset ); }

        std::pair< Node*, size_t > get_next_node()
        {
            OTK_ASSERT( bytes_free() > sizeof( Node ) );
            size_t offset = next_node();
            next_node() += sizeof( Node );
            new ( ptr() + offset ) Node();
            return std::make_pair( node_ptr( offset ), offset );
        }

        Record& record( const size_t offset ) { OTK_ASSERT( offset >= next_record() ); return *record_ptr( offset ); }
        const Record& record( const size_t offset ) const { OTK_ASSERT( offset >= next_record() ); return *record_ptr( offset ); }

        std::pair< Record*, size_t > get_next_record()
        {
            OTK_ASSERT( bytes_free() > record_size );
            size_t offset = next_record();
            next_record() -= record_size;
            new ( ptr() + offset ) Record();
            return std::make_pair( record_ptr( offset ), offset );
        }

        static constexpr size_t size() { return BlockSize; }

        size_t bytes_free() const { return next_record() + record_size - next_node(); }

        size_t index() const { return m_dataBlock->index; }

    private:
        static constexpr size_t next_node_offset() { return size() - sizeof( std::atomic_size_t ); }
        static constexpr size_t next_record_offset() { return next_node_offset() - sizeof( std::atomic_size_t ); }
        static constexpr size_t snap_id_offset() { return next_record_offset() - sizeof( std::size_t ); }

        char* ptr() { return ( char* )( m_dataBlock->get_data() ); }

        std::atomic_size_t* next_node_ptr() { return ( std::atomic_size_t* )( ptr() + next_node_offset() ); }
        std::atomic_size_t* next_record_ptr() { return ( std::atomic_size_t* )( ptr() + next_record_offset() ); }
        std::size_t* snap_id_ptr() { return ( size_t* )( ptr() + snap_id_offset() ); }

        Node* node_ptr( const size_t offset ) { return reinterpret_cast<Node*>( ptr() + offset ); }
        Record* record_ptr( const size_t offset ) { return reinterpret_cast<Record*>( ptr() + offset ); }

        std::atomic_size_t& next_node() { return *next_node_ptr(); }
        std::atomic_size_t& next_record() { return *next_record_ptr(); }
        size_t& snapshot_id() { return *snap_id_ptr(); }

        std::shared_ptr< DataBlock > m_dataBlock;
    };

public:
    TableWriter( std::shared_ptr< BlockFile > data_file, std::shared_ptr< const Snapshot > latest_snapshot )
        : m_dataFile( data_file )
    {
        if( latest_snapshot )
        {
            m_currentSnapshot.setId( latest_snapshot->m_snapshotId + 1 );
            m_currentSnapshot.setRoot( latest_snapshot->m_rootIndex );
        }
        else
        {
            m_currentSnapshot.setId( 0 );
            m_currentSnapshot.setRoot( 0 );
        }
    }

    void Insert( const Key& key, const Record& record )
    {
        Link current = get_root_link();
        Link parent  = get_root_link();
        
        TableDataBlock curr_block = get_block( current.getBlock() );
        do
        {
            Node* curr_node = curr_block.node( current.getLocalAddress() );

            // Traverse side links.
            while( curr_node->side_link().first && key >= curr_node->side_link().first )
            {
                if( current.getBlock() != curr_node->side_link().second.getBlock() )
                    curr_block = get_block( curr_node->side_link().second.getBlock() );

                current = curr_node->side_link().second;
                curr_node = curr_block.node( current.getLocalAddress() );
            }

            if( curr_node->isFull() )
                if( current == parent &&
                    current != get_root_link() )
                {
                    current = parent = get_root_link();
                }

            if( curr_node->isFull() || curr_node->isLeaf() )
            {
                if( !curr_node->tryLatch() )
                {
                    if( current.getBlock() != parent.getBlock() )
                        curr_block = get_block( parent.getBlock() );
                    current = parent;
                    continue;
                }
            }

            if( curr_node->isFull() )
            {
                bool result = trySplitAndUpdateParent( current, parent );
                if( result == true && !curr_node->isLeaf() )
                    curr_node->unlatch();
                else if( result == parent_full || result == unknown )
                {
                    curr_node->unlatch();
                    if( current.getBlock() != parent.getBlock() )
                        curr_block = get_block( parent.getBlock() );
                    current = parent;
                    continue;
                }
            }

            const Pair_T& pair = curr_node->find( key );

            if( curr_node->isLeaf() )
            {
                if( pair.first != key )
                {
                    auto new_pair = curr_block.get_next_record();
                    *new_pair.first = record;
                    curr_node->insert( Pair_T( key, Link( true, true, current.getBlock(), new_pair.second ) ) );
                }

                curr_node->unlatch();
            }
            else
            {
                OTK_ASSERT( pair.second.isValid() );
                OTK_ASSERT( !pair.second.isLeaf() );
                OTK_ASSERT( pair.first < key );

                if( current.getBlock() != pair.second.getBlock() )
                    curr_block = get_block( pair.second.getBlock() );

                current = pair.second;
            }
        } while( !current.isLeaf() );
        //current = parent = root.root();
        //if( node. )
        // Get Root block.
        // Traverse to leaf.
        // If node full, split.
        // Insert into leaf.
    }

    void Erase( const Key& key )
    {

    }

    Snapshot&& TakeSnaphot()
    {
        m_dataFile->flush( false );

        Snapshot snapshot( std::move( m_currentSnapshot ) );

        m_currentSnapshot.setId( snapshot.m_snapshotId + 1 );
        m_currentSnapshot.setRoot( snapshot.m_rootIndex );

        return std::move( snapshot );
    }

    // No copying
    TableWriter( const TableWriter& o ) = delete;
    
private:
    size_t copy_on_write( const size_t block_index )
    {
        auto old_block = m_dataFile->checkOutForRead( block_index );
        auto new_block = m_dataFile->checkOutNewBlock();

        *new_block = *old_block;
        TableDataBlock block( new_block );
        block.set_snapshot( m_currentSnapshot );
        m_currentSnapshot.m_modifiedBlocks.insert( old_block->index );
        m_currentSnapshot.m_newBlocks.insert( new_block->index );

        return new_block->index;
    }

    TableDataBlock get_block( size_t index )
    {
        OTK_ASSERT( m_dataFile->exists( index ) );
        if( !m_currentSnapshot.is_new( index ) )
        {
            index = copy_on_write( index );
        }

        return TableDataBlock( m_dataFile->checkOutForReadWrite( index ) );
    }

    TableDataBlock get_new_block()
    {
        TableDataBlock new_block( m_dataFile->checkOutNewBlock() );
        new_block.init( m_currentSnapshot );
        m_currentSnapshot.m_newBlocks.insert( new_block.index() );
        return new_block;
    }

    TableDataBlock get_root_block()
    {
        if( !m_dataFile->exists( m_currentSnapshot.m_rootIndex ) )
        {
            auto new_block = get_new_block();
            OTK_ASSERT( new_block.index() == m_currentSnapshot.m_rootIndex );
            return new_block;
        }
        else if( !m_currentSnapshot.is_new( m_currentSnapshot.m_rootIndex ) )
        {
            m_currentSnapshot.setRoot( copy_on_write( m_currentSnapshot.m_rootIndex ) );
        }

        return TableDataBlock( m_dataFile->checkOutForReadWrite( m_currentSnapshot.m_rootIndex ) );
    }

    Link get_root_link()
    {
        if( !m_root_link.isValid() )
        {
            TableDataBlock root_block = get_root_block();
            const Node& root_node = root_block.root();

            m_root_link.setLocal( false );
            m_root_link.setBlock( m_currentSnapshot.m_rootIndex );
            m_root_link.setLocalAddress( 0 );

            if( root_node.isValid() )
                m_root_link.setLeaf( root_node.isLeaf() );
            else
                m_root_link.setLeaf( true ); // This only happens for the first root node created, which is always a leaf at first.
        }

        return m_root_link;
    }

    Link                         m_root_link;
    Snapshot                     m_currentSnapshot;
    std::shared_ptr< BlockFile > m_dataFile;
    std::mutex                   m_mutex;
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

                snapshot->setId( id );
                snapshot->setRoot( rt );
                std::copy( in_data.begin(), in_data.begin() + nc,
                           std::inserter( snapshot->m_newBlocks, snapshot->m_newBlocks.end() ) );
                std::copy( in_data.begin() + nc, in_data.begin() + nc + mc,
                           std::inserter( snapshot->m_modifiedBlocks, snapshot->m_modifiedBlocks.end() ) );
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
        const size_t rt = snap->m_rootIndex;

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
