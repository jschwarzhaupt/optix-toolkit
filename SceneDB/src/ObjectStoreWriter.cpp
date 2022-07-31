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

#include "ObjectFileWriter.h"

#include <OptiXToolkit/SceneDB/ObjectStoreWriter.h>
#include <OptiXToolkit/Util/Exception.h>

#include <filesystem>

namespace otk {

ObjectStoreWriter::ObjectStoreWriter( const char* directory, size_t bufferSize, bool discardDuplicates )
{
    OTK_ASSERT_MSG( bufferSize == 0, "ObjectStoreWriter buffering is TBD" );
    OTK_ASSERT_MSG( discardDuplicates == false, "ObjectStoreWriter deduplication is TBD" );

    std::filesystem::create_directory( directory ); // throws on error

    std::filesystem::path filename( std::filesystem::path(directory) / "objects.dat" );
    m_file.reset( new ObjectFileWriter( filename.string().c_str() ) );

    synchronize();
}

ObjectStoreWriter::~ObjectStoreWriter()
{
    // The file is closed by ~ObjectFileWriter.
}

void ObjectStoreWriter::synchronize()
{
    m_file->synchronize();
}

}  // namespace otk
