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
#include <OptiXToolkit/Util/Exception.h>

#include <errno.h>
#include <fcntl.h>
#include <vector>

#ifdef WIN32
#include <fileapi.h>
#else
#include <sys/uio.h>
#include <sys/file.h>
#include <unistd.h>
#endif

namespace sceneDB {

BlockFile::BlockFile( const std::filesystem::path& path, 
                      std::shared_ptr<Header> header,
                      const size_t block_size,
                      const size_t block_alignment,
                      const bool request_write )
    : 
#ifdef WIN32
      m_file( INVALID_HANDLE_VALUE )
#else
      m_file( -1 )
#endif
    , m_blockSize( block_size )
    , m_blockAlignment( block_alignment )
    , m_header( header )
{
    // File permissions
#ifdef WIN32
    // Try to open file for writing, if requested.
    if( request_write )
        m_file = CreateFileA( path.string().c_str(), GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ, nullptr, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OVERLAPPED, nullptr );

    if( m_file == INVALID_HANDLE_VALUE )
    {
        int code = GetLastError();
        if( request_write == false ||
            code == ERROR_SHARING_VIOLATION )
        {
            // Open file read-only if we couldn't open for writing, or writing wasn't requested.
            m_file = CreateFileA( path.string().c_str(), GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OVERLAPPED, nullptr );
            if( m_file == INVALID_HANDLE_VALUE )
                code = GetLastError();
            else
                code = 0;
        }

        if( code )
            throw otk::Exception( "Cannot open BlockFile", code );
    }
    else
        m_writeable = true;
#else
    mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH;

    // Try to open file for writing, if requested.
    // Check that we can flock() the file exclusively. If not, then open read-only.
    m_file = open( path.c_str(), (request_write ? O_RDWR : O_RDONLY) | O_CREAT, mode );
    if( m_file < 0 )
    {
        int code = errno;
        throw otk::Exception( "Cannot open BlockFile", code );
    }

    if( request_write )
    {
        int result = flock( m_file, LOCK_EX | LOCK_NB );
        if( !result )
            m_writeable = true;
    }
#endif
    m_header->m_version        = Header::k_version;
    m_header->m_blockSize      = block_size;
    m_header->m_blockAlignment = block_alignment;

    if( !checkHeader() )
        throw otk::Exception( "BlockFile is incompatible." );

    if( m_writeable )
        writeHeader();
}

BlockFile::~BlockFile()
{
#ifdef WIN32
    CloseHandle( m_file );
#else
    close( m_file );
#endif    
}

std::shared_ptr<const DataBlock> BlockFile::checkOutForRead( const size_t index )
{
    OTK_ASSERT_MSG( index < m_nextBlock.load() && m_freeBlockIndices.count( index ) == 0, "Attempt to check out an invalid block." );
    OTK_ASSERT_MSG( m_writeBlocks.count( index ) == 0, "Attempt to check out for reading a block already checked out for writing." );

    auto it = m_readBlocks.find( index );
    if( it != m_readBlocks.end() )
        return it->second;

    std::unique_ptr<DataBlock> new_block( new DataBlock( m_blockSize, m_blockAlignment, index ) );
    const offset_t offset = getBlockOffset( index );

#ifdef WIN32
    OTK_ASSERT( m_file != INVALID_HANDLE_VALUE );
    DWORD bytes_to_read = m_blockSize;

    OVERLAPPED ol;
    ol.Internal = 0;
    ol.InternalHigh = 0;
    ol.Offset = offset & 0xFFFFFFFFll;
    ol.OffsetHigh = DWORD( offset >> 32 );
    ol.hEvent = NULL;

    auto result = ReadFileEx( m_file, new_block->get_data(), bytes_to_read, &ol, NULL );
    if( !result ||
        SleepEx( INFINITE, true ) != WAIT_IO_COMPLETION )
    {
        return false;
    }

    DWORD bytes_read = 0;
    auto ol_res = GetOverlappedResult( m_file, &ol, &bytes_read, false );
    auto le_res = GetLastError();

    if( !ol_res ||
        bytes_read != bytes_to_read )
    {
        throw otk::Exception( "Error attempting to read block from file.", le_res );
    }
#else
    OTK_ASSERT( m_file >= 0 );
    auto result = pread( m_file, new_block->get_data(), m_blockSize, offset );
    if( result != m_blockSize )
    {
        throw otk::Exception( "Error attempting to read block from file.", errno );
    }
#endif

    new_block->set_valid( true );

    it = m_readBlocks.insert( { index, std::move( new_block ) } ).first;

    return it->second;
}

std::shared_ptr<DataBlock> BlockFile::checkOutForReadWrite( const size_t index )
{
    OTK_ASSERT_MSG( index < m_nextBlock.load() && m_freeBlockIndices.count( index ) == 0, "Attempt to check out an invalid block." );
    OTK_ASSERT_MSG( m_readBlocks.count( index ) == 0, "Attempt to check out for writing a block already checked out for reading." );

    auto it = m_writeBlocks.find( index );
    if( it != m_writeBlocks.end() )
        return it->second;

    std::unique_ptr<DataBlock> new_block( new DataBlock( m_blockSize, m_blockAlignment, index ) );
    const offset_t offset = getBlockOffset( index );

#ifdef WIN32
    OTK_ASSERT( m_file != INVALID_HANDLE_VALUE );
    DWORD bytes_to_read = m_blockSize;

    OVERLAPPED ol;
    ol.Internal = 0;
    ol.InternalHigh = 0;
    ol.Offset = offset & 0xFFFFFFFFll;
    ol.OffsetHigh = DWORD( offset >> 32 );
    ol.hEvent = NULL;

    auto result = ReadFileEx( m_file, new_block->get_data(), bytes_to_read, &ol, NULL );
    if( !result ||
        SleepEx( INFINITE, true ) != WAIT_IO_COMPLETION )
    {
        return false;
    }

    DWORD bytes_read = 0;
    auto ol_res = GetOverlappedResult( m_file, &ol, &bytes_read, false );
    auto le_res = GetLastError();

    if( !ol_res ||
        bytes_read != bytes_to_read )
    {
        throw otk::Exception( "Error attempting to read block from file.", le_res );
    }
#else
    OTK_ASSERT( m_file >= 0 );
    auto result = pread( m_file, new_block->get_data(), m_blockSize, offset );
    if( result != m_blockSize )
    {
        throw otk::Exception( "Error attempting to read block from file.", errno );
    }
#endif

    new_block->set_valid( true );

    it = m_writeBlocks.insert( { index, std::move( new_block ) } ).first;

    return it->second;
}

std::shared_ptr<DataBlock> BlockFile::checkOutNewBlock()
{
    OTK_ASSERT( m_writeable );

    size_t block_index;
    if( m_freeBlockIndices.size() )
    {
        std::lock_guard guard( m_mutex );
        auto it = m_freeBlockIndices.begin();
        block_index = *it;
        m_freeBlockIndices.erase( it );
    }
    else
    {
        block_index = m_nextBlock++;

        // Make sure we can store the new block later.
        // Seek to where the end of this new block would be.
        // Add size of storage for free-list as well.
        std::lock_guard guard( m_mutex );
        const offset_t offset = m_freeBlockIndices.size() * sizeof( size_t ) + getBlockOffset( block_index + 1 ) - 1;

#ifdef WIN32
        OTK_ASSERT( m_file != INVALID_HANDLE_VALUE );
        LARGE_INTEGER _offset;
        _offset.QuadPart = offset;

        auto result = SetFilePointerEx( m_file, _offset, NULL, FILE_BEGIN );
        if( result )
            result = SetEndOfFile( m_file );

        if( !result )
            throw otk::Exception( "Error attempting to increase the size of file.", result );
#else
        OTK_ASSERT( m_file >= 0 );
        auto result = ftruncate( m_file, offset );
        if( result )
            throw otk::Exception( "Error attempting to increase the size of file.", errno );
#endif
    }

    auto it = m_writeBlocks.emplace( std::make_pair( block_index, std::make_shared<DataBlock>(m_blockSize, m_blockAlignment, block_index) ) ).first;
    it->second->set_valid( true );

    return it->second;
}

void BlockFile::flush( bool flush_caches )
{
    OTK_ASSERT( isWriteable() );
    std::lock_guard guard( m_mutex );

    // Write it all to disk
    for( auto& elt : m_writeBlocks )
    {
        elt.second->set_valid( false );
        writeBlock( elt.second );
    }

    m_writeBlocks.clear();

    writeHeader();

    if( flush_caches )
    {
#ifdef WIN32
        OTK_ASSERT( m_file != INVALID_HANDLE_VALUE );
        auto result = FlushFileBuffers( m_file );
        if( !result )
            throw otk::Exception( "Error flushing file caches.", GetLastError() );
#else
        OTK_ASSERT( m_file >= 0 );
        auto result = fsync( m_file );
        if( result )
            throw otk::Exception( "Error flushing file caches.", errno );
#endif
    }
}

void BlockFile::unloadBlock( const size_t index )
{
    auto r_it = m_readBlocks.find( index );
    if( r_it != m_readBlocks.end() )
    {
        r_it->second->set_valid( false );
        m_readBlocks.erase( r_it );
        return;
    }

    auto w_it = m_writeBlocks.find( index );
    if( w_it != m_writeBlocks.end() )
    {
        OTK_ASSERT( isWriteable() );
        w_it->second->set_valid( false );
        writeBlock( w_it->second );
        m_writeBlocks.erase( w_it );
    }
}

void BlockFile::freeBlock( const size_t index )
{
    OTK_ASSERT( isWriteable() );
    auto r_it = m_readBlocks.find( index );
    if( r_it != m_readBlocks.end() )
    {
        r_it->second->set_valid( false );
        m_readBlocks.erase( r_it );
    }

    m_freeBlockIndices.insert( index );
}

bool BlockFile::checkHeader()
{
    std::unique_ptr<Header> file_header = m_header->getInstance();
    std::vector<char> file_header_data( file_header->getSize() );
    bool read_ok = false;

#ifdef WIN32
    OTK_ASSERT( m_file != INVALID_HANDLE_VALUE );
    DWORD bytes_to_read = file_header_data.size();

    OVERLAPPED ol;
    ol.Internal = 0;
    ol.InternalHigh = 0;
    ol.Offset = 0;
    ol.OffsetHigh = 0;
    ol.hEvent = NULL;

    auto result = ReadFileEx( m_file, file_header_data.data(), bytes_to_read, &ol, NULL );
    if( !result ||
        SleepEx( INFINITE, true ) != WAIT_IO_COMPLETION )
    {
        return false;
    }

    DWORD bytes_read = 0;
    auto ol_res = GetOverlappedResult( m_file, &ol, &bytes_read, false );
    auto le_res = GetLastError();

    if( ol_res &&
        bytes_read == bytes_to_read )
    {
        read_ok = true;
    }
    else if( le_res != ERROR_HANDLE_EOF )
        return false;
#else
    OTK_ASSERT( m_file >= 0 );
    auto result = pread( m_file, file_header_data.data(), file_header_data.size(), 0 );
    if( result == file_header_data.size() )
    {
        read_ok = true;
    }
    else if( result < 0 )
        return false;
#endif
    if( !read_ok )
    {
        // No header. This is a new file.
        return true;
    }

    file_header->deserialize( file_header_data.data() );

    if( read_ok &&
        m_header->check( file_header.get() ) )
    {
        m_header->deserialize( file_header_data.data() );
        m_nextBlock.store( m_header->m_nextBlock );

        // Readers don't care about the free list so don't bother reading it.
        if( m_writeable &&
            m_header->m_freeListSize )
        {
            read_ok = false;

            const size_t   size_in_bytes = m_header->m_freeListSize * sizeof( size_t );
            const offset_t offset        = getBlockOffset( m_nextBlock.load() + 1 ); // We've stored the free list at EOF.

            std::vector<size_t> free_list( m_header->m_freeListSize, ~0llu );

#ifdef WIN32
            ol.Internal = 0;
            ol.InternalHigh = 0;
            ol.Offset = offset & 0xFFFFFFFFll;
            ol.OffsetHigh = DWORD( offset >> 32 );
            ol.hEvent = NULL;

            auto result = ReadFileEx( m_file, free_list.data(), size_in_bytes, &ol, NULL );
            if( result &&
                SleepEx( INFINITE, true ) == WAIT_IO_COMPLETION )
            {
                bytes_read = 0;
                auto ol_res = GetOverlappedResult( m_file, &ol, &bytes_read, false );

                if( ol_res &&
                    bytes_read == size_in_bytes )
                {
                    read_ok = true;
                }
            }
#else
            auto result = pread( m_file, free_list.data(), size_in_bytes, offset );
            if( result == size_in_bytes )
            {
                read_ok = true;
            }
#endif

            if( read_ok )
            {
                for( auto i : free_list )
                    if( i != ~0llu )
                        m_freeBlockIndices.insert( i );
            }
        }

        return true;
    }

    return false;
}

void BlockFile::writeHeader() const
{
    if( m_writeable )
    {
        m_header->m_nextBlock    = m_nextBlock;
        m_header->m_freeListSize = m_freeBlockIndices.size();

        std::vector<char> header_data( m_header->getSize() );
        m_header->serialize( header_data.data() );

#ifdef WIN32
        OTK_ASSERT( m_file != INVALID_HANDLE_VALUE );
        DWORD bytes_to_write = header_data.size();

        OVERLAPPED ol;
        ol.Internal = 0;
        ol.InternalHigh = 0;
        ol.Offset = 0;
        ol.OffsetHigh = 0;
        ol.hEvent = NULL;

        auto result = WriteFileEx( m_file, header_data.data(), bytes_to_write, &ol, NULL );
        if( !result ||
            SleepEx( INFINITE, true ) != WAIT_IO_COMPLETION )
        {
            throw otk::Exception( "BlockFile error writing header." );
        }

        DWORD bytes_written = 0;
        auto ol_res = GetOverlappedResult( m_file, &ol, &bytes_written, false );
        auto le_res = GetLastError();

        if( !ol_res ||
            bytes_written != bytes_to_write )
        {
            throw otk::Exception( "BlockFile error writing header." );
        }
#else
        OTK_ASSERT( m_file >= 0 );
        auto result = pwrite( m_file, header_data.data(), header_data.size(), 0 );
        if( result != header_data.size() )
        {
            throw otk::Exception( "BlockFile error writing header." );
        }
#endif

        if( m_freeBlockIndices.size() )
        {
            std::vector<size_t> flat_array( m_freeBlockIndices.begin(), m_freeBlockIndices.end() );

            const size_t   size_in_bytes = flat_array.size() * sizeof( size_t );
            const offset_t offset        = getBlockOffset( m_nextBlock.load() + 1 ); // Store the free list at EOF.

#ifdef WIN32
            bytes_to_write = size_in_bytes;

            OVERLAPPED ol;
            ol.Internal = 0;
            ol.InternalHigh = 0;
            ol.Offset = offset & 0xFFFFFFFFll;
            ol.OffsetHigh = DWORD( offset >> 32 );
            ol.hEvent = NULL;

            result = WriteFileEx( m_file, flat_array.data(), bytes_to_write, &ol, NULL );
            if( !result ||
                SleepEx( INFINITE, true ) != WAIT_IO_COMPLETION )
            {
                throw otk::Exception( "BlockFile error writing free list." );
            }

            DWORD bytes_written = 0;
            auto ol_res = GetOverlappedResult( m_file, &ol, &bytes_written, false );
            auto le_res = GetLastError();

            if( !ol_res ||
                bytes_written != bytes_to_write )
            {
                throw otk::Exception( "BlockFile error writing header." );
            }
#else
            result = pwrite( m_file, flat_array.data(), size_in_bytes, offset );
            if( result != size_in_bytes )
            {
                throw otk::Exception( "BlockFile error writing header." );
            }
#endif
        }
    }    
}

void BlockFile::writeBlock( std::shared_ptr<DataBlock> block )
{
    const offset_t offset = getBlockOffset( block->index );

#ifdef WIN32
    OTK_ASSERT( m_file != INVALID_HANDLE_VALUE );
    DWORD bytes_to_write = m_blockSize;

    OVERLAPPED ol;
    ol.Internal = 0;
    ol.InternalHigh = 0;
    ol.Offset = offset & 0xFFFFFFFFll;
    ol.OffsetHigh = DWORD( offset >> 32 );
    ol.hEvent = NULL;

    auto result = WriteFileEx( m_file, block->get_data(), bytes_to_write, &ol, NULL );
    if( !result ||
        SleepEx( INFINITE, true ) != WAIT_IO_COMPLETION )
    {
        throw otk::Exception( "BlockFile error writing to file." );
    }

    DWORD bytes_written = 0;
    auto ol_res = GetOverlappedResult( m_file, &ol, &bytes_written, false );
    auto le_res = GetLastError();

    if( !ol_res ||
        bytes_written != bytes_to_write )
    {
        throw otk::Exception( "BlockFile error writing to file." );
    }
#else
    auto result = pwrite( m_file, block->get_data(), m_blockSize, offset );
    if( result != m_blockSize )
    {
        throw otk::Exception( "BlockFile error writing to file." );
    }
#endif
}

}  // namespace sceneDB
