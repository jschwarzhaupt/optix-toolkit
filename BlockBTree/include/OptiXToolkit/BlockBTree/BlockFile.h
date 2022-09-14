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

#include <OptiXToolkit/BlockBTree/DataBlock.h>

#include <atomic>
#include <cstddef>
#include <sys/types.h>
#include <filesystem>
#include <map>
#include <set>

#ifdef WIN32
#include <windows.h>
#endif

namespace sceneDB {

/**
* A lock-free, thread-safe, block-based file. A given file on disk can only
* be opened for writing by one BlockFile object. Multiple processes may
* open the file for reading.
* Only entire blocks may be read / written.
*/
class BlockFile
{
public:
    class Header
    {
    public:
        Header() = default;

        virtual bool check( const Header* header ) const
        {
            return m_version == header->m_version &&
                m_blockSize == header->m_blockSize &&
                m_blockAlignment == header->m_blockAlignment;
        }

        virtual size_t getSize() const
        {
            return 5 * sizeof( size_t );
        }

        virtual void serialize( void* buf ) const
        {
            size_t* _buf = ( size_t* )buf;
            _buf[ 0 ] = m_version;
            _buf[ 1 ] = m_blockSize;
            _buf[ 2 ] = m_blockAlignment;
            _buf[ 3 ] = m_nextBlock;
            _buf[ 4 ] = m_freeListSize;
        }

        virtual void deserialize( void* buf )
        {
            size_t* _buf = ( size_t* )buf;
            m_version = _buf[ 0 ];
            m_blockSize = _buf[ 1 ];
            m_blockAlignment = _buf[ 2 ];
            m_nextBlock = _buf[ 3 ];
            m_freeListSize = _buf[ 4 ];
        }

        virtual std::unique_ptr<Header> getInstance() const
        {
            return std::make_unique<Header>();
        }

        static constexpr size_t k_version = 1;

        size_t m_version{ k_version };
        size_t m_blockSize{ 0 };
        size_t m_blockAlignment{ 0 };
        size_t m_nextBlock{ 0 };
        size_t m_freeListSize{ 0 };
    };

#ifdef WIN32
      typedef long long  offset_t;
#else
      typedef off_t      offset_t;
#endif

    /// Construct BlockFile object, creating or opening the specified file. 
    /// If the file exists, it's header is checked to validate that the block size
    /// and alignment match the passed parameters.
    /// If request_write is true, attempts to open the file with write permissions.
    /// Note that the file may be opened in read-only mode even if write permission
    /// was requested (eg, if another process has already opened the file for writing).
    /// Throws an exception if an error occurs.
    explicit BlockFile( const std::filesystem::path& path, 
                        std::shared_ptr<Header> header,
                        const size_t block_size, 
                        const size_t block_alignment, 
                        const bool request_write );

    /// Destroy BlockFile object, closing the associated file. Any outstanding blocks
    /// checked out for writing are flushed to disk. All checked out blocks are invalidated.
    ~BlockFile();

    bool exists( const size_t index ) { return index < m_nextBlock; }

    /// Check out a block for read-only access.
    /// Blocks may only be checked out for read-only OR read/write access, not both.
    /// Concurrent requests for the same block return the same object via a shared_ptr.
    /// Attempting to check out a nonexistent block ( ie, index >= getNextBlockIndex() )
    /// will throw an exception.
    /// This will load the block from disk the first time it's checked out,
    /// or after a block has been unloaded.
    std::shared_ptr<const DataBlock> checkOutForRead( const size_t index );

    /// Check out a block for read/write access.
    /// Blocks may only be checked out for read-only OR read/write access, not both.
    /// Concurrent requests for the same block return the same object via a shared_ptr.
    /// Attempting to check out a nonexistent block ( ie, index >= getNextBlockIndex() )
    /// will throw an exception.
    /// This will load the block from disk the first time it's checked out,
    /// or after a block has been unloaded.
    std::shared_ptr<DataBlock> checkOutForReadWrite( const size_t index );

    /// Check out the next available block for read/write access.
    /// This can be a new block appended to the end of the file,
    /// or a previously 
    std::shared_ptr<DataBlock> checkOutNewBlock();

    /// Write all checked out read/write blocks to disk and update the header. 
    /// If flush_caches is true, also flushes any file system caches.
    void flush( bool flush_caches = false );

    /// Invalidate the specified block and remove it from the appropriate
    /// map of checked-out blocks (read or read/write).
    /// The block is not added to the free list.
    /// If the block was loaded for read/write access, then it is
    /// written to disk prior to unloading.
    void unloadBlock( const size_t index );

    /// Invalidate the specified block and remove it from the map of blocks checked out
    /// for reading (if it has been checked out). Add the block to the free list.
    /// The block becomes available for checking out for read/write access.
    void freeBlock( const size_t index );

    /// Returns whether the file was opened with write access.
    bool isWriteable() const { return m_writeable;  }

    /// Copying is prohibited.
    BlockFile(const BlockFile&) = delete;

    /// Assignment is prohibited.
    BlockFile& operator=(const BlockFile&) = delete;

private:
    bool checkHeader();
    void writeHeader() const;

    offset_t getBlockOffset( const size_t index ) const
    {
        return ( 1 + index ) * m_blockSize;
    }

    void writeBlock( std::shared_ptr<DataBlock> block );

#ifdef WIN32
    HANDLE              m_file;
#else
    int                 m_file;
#endif

    std::map<size_t, std::shared_ptr<DataBlock>>        m_writeBlocks;
    std::map<size_t, std::shared_ptr<const DataBlock>>  m_readBlocks;
    std::set<size_t>                                    m_freeBlockIndices;

    const size_t            m_blockSize;
    const size_t            m_blockAlignment;
    bool                    m_writeable{ false };
    std::mutex              m_mutex;
    std::atomic<size_t>     m_nextBlock{ 0 };
    std::shared_ptr<Header> m_header;
};

}  // namespace sceneDB
