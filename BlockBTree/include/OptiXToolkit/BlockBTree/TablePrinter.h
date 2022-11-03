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

#include <OptiXToolkit/BlockBTree/Table.h>

#include <iostream>

namespace sceneDB {

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
class TablePrinter : public TableReader<Key, Record, B, BlockSize, BlockAlignment>
{
  public:
    using TableReader = class TableReader<Key, Record, B, BlockSize, BlockAlignment>;
    typedef Table<Key, Record, B, BlockSize, BlockAlignment> TableType;
    typedef typename TableType::Node       Node;
    typedef typename TableType::Node::Link Link;

    TablePrinter( std::shared_ptr<BlockFile> data_file, const size_t root_index, const size_t root_local_offset )
        : TableReader( data_file, root_index, root_local_offset )
    {
    }

    void Print( std::ostream& out );

  private:
    void PrintNode( std::ostream& out, Link link );
};

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
void TablePrinter<Key, Record, B, BlockSize, BlockAlignment>::Print( std::ostream& out )
{
    // Start DOT graph
    out << "digraph G {" << std::endl;
    out << "  node [shape=record]" << std::endl;

    // Print an incoming edge to the root node, to ensure that it's rendered if it's empty.
    Link link = TableReader::m_root_link;
    out << "  root [shape=none,label=\"\"]" << std::endl;
    out << "  root -> " << link.m_link << std::endl;

    // Recursively print nodes.
    PrintNode( out, link );

    // End DOT graph.
    out << "}" << std::endl;
}

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
void TablePrinter<Key, Record, B, BlockSize, BlockAlignment>::PrintNode( std::ostream& out, Link link )
{
    TableReaderDataBlock block = TableReader::get_block( link.get_block() );
    const Node*          node  = block.node( link.get_local_address() );
    // Print record node labelled "<1>|...|<B-1>"
    // (note that pair zero is reserved)
    out << "  " << link.m_link << " [label=\"<1>";
    for( unsigned int i = 2; i < node->m_valid_count; ++i )
    {
        out << "|<" << i << ">";
    }
    out << "\"]" << std::endl;

    if( node->is_leaf() )
    {
        return;
    }

    // Print an edge for each child, of the form "P:i:s -> C:n", where
    // P=parent, i=index, s=south, C=child, n=north
    for( unsigned int i = 1; i < node->m_valid_count; ++i )
    {
        Link child = node->m_pairs[i].second;
        out << "  " << link.m_link << ":" << i << ":s -> " << child.m_link << ":n" << std::endl;
    }

    // Recursively print the child nodes.
    for( unsigned int i = 1; i < node->m_valid_count; ++i )
    {
        Link child = node->m_pairs[i].second;
        PrintNode( out, child );
    }
}

}  // namespace sceneDB
