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
#include <map>

namespace sceneDB {

/// Print table as a DOT graph.  Use Table::getPrinter() to obtain a TablePrinter.
/// The dot executable is part of the Graphviz package: https://graphviz.org/download/
/// Use "dot -Tpdf filename.dot" to render a DOT graph to PDF.
template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
class TablePrinter : public TableReader<Key, Record, B, BlockSize, BlockAlignment>
{
  public:
    typedef class TableReader<Key, Record, B, BlockSize, BlockAlignment> TableReader;
    typedef Table<Key, Record, B, BlockSize, BlockAlignment> TableType;
    typedef typename TableType::Node       Node;
    typedef typename TableType::Node::Link Link;

    TablePrinter( std::shared_ptr<BlockFile> data_file, const size_t root_index, const size_t root_local_offset )
        : TableReader( data_file, root_index, root_local_offset )
    {
    }

    void Print( std::ostream& out );

  private:
    void GatherBlocks( std::map<size_t, std::vector<size_t>>& blocks, Link link );
    void PrintBlocks( std::map<size_t, std::vector<size_t>>& blocks, std::ostream& out );
    void PrintEdges( std::ostream& out, Link link );
};

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
void TablePrinter<Key, Record, B, BlockSize, BlockAlignment>::Print( std::ostream& out )
{
    // Start DOT graph.  See https://graphviz.org/doc/info/lang.html
    out << "digraph G {" << std::endl;
    out << "  layout=dot" << std::endl;
    out << "  node [shape=record, style=filled, color=black, fillcolor=white]" << std::endl;

    // Map from block to vector of node offsets.
    std::map<size_t, std::vector<size_t>> blocks;

    // Gather a vector of node offsets for each block.
    GatherBlocks( blocks, TableReader::m_root_link );

    // Print the nodes of each block as a subgraph.
    PrintBlocks( blocks, out );

    // Print the edges.
    PrintEdges( out, TableReader::m_root_link );

    // End DOT graph.
    out << "}" << std::endl;
}

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
void TablePrinter<Key, Record, B, BlockSize, BlockAlignment>::GatherBlocks( std::map<size_t, std::vector<size_t>>& blocks, Link link )
{
    // Add this node to the vector of nodes for the corresponding block.
    blocks[link.get_block()].push_back( link.get_local_address() );

    // Recursively gather child nodes.
    TableReaderDataBlock block = TableReader::get_block( link.get_block() );
    const Node*          node  = block.node( link.get_local_address() );
    if( !node->is_leaf() )
    {
        for( unsigned int i = 0; i < node->m_valid_count; ++i )
        {
            Link child = node->m_pairs[i].second;
            GatherBlocks( blocks, child );
        }
    }
}

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
void TablePrinter<Key, Record, B, BlockSize, BlockAlignment>::PrintBlocks( std::map<size_t, std::vector<size_t>>& blocks,
                                                                           std::ostream& out )
{
    for( auto it = blocks.begin(); it != blocks.end(); ++it )
    {
        size_t               block_offset = it->first;
        TableReaderDataBlock block        = TableReader::get_block( block_offset );

        // Begin DOT subgraph.  See https://graphviz.org/Gallery/directed/cluster.html
        out << "  subgraph cluster_" << block_offset << " {" << std::endl;
        out << "    style=filled" << std::endl;
        out << "    color=lightgrey" << std::endl;

        for( size_t node_offset : it->second )
        {
            const Node* node = block.node( node_offset );

            // Print record node labelled "<0>|...|<N-1>" where N is the number of children.
            // See https://graphviz.org/doc/info/shapes.html#record
            out << "    "
                << "node_" << block_offset << "_" << node_offset << " [label=\"<0>";
            for( unsigned int i = 1; i < node->m_valid_count; ++i )
            {
                out << "|<" << i << ">";
            }
            out << "\"]" << std::endl;
        }

        // End DOT subgraph
        out << "  }" << std::endl;
    }
}

template <typename Key, class Record, size_t B, size_t BlockSize, size_t BlockAlignment>
void TablePrinter<Key, Record, B, BlockSize, BlockAlignment>::PrintEdges( std::ostream& out, Link link )
{
    TableReaderDataBlock block = TableReader::get_block( link.get_block() );
    const Node*          node  = block.node( link.get_local_address() );

    if( node->is_leaf() )
    {
        return;
    }

    // Print an edge for each child, of the form "P:i:s -> C:n", where
    // P=parent, i=index, s=south, C=child, n=north
    for( unsigned int i = 0; i < node->m_valid_count; ++i )
    {
        Link child = node->m_pairs[i].second;

        out << "  "
            << "node_" << link.get_block() << "_" << link.get_local_address() << ":" << i << ":s -> "
            << "node_" << child.get_block() << "_" << child.get_local_address() << ":n" << std::endl;
    }

    // Recursively print edges from child nodes.
    for( unsigned int i = 0; i < node->m_valid_count; ++i )
    {
        Link child = node->m_pairs[i].second;
        PrintEdges( out, child );
    }
}

}  // namespace sceneDB
