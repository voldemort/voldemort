/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.store.readonly.mr;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import voldemort.store.readonly.chunk.DataFileChunk;

public class HdfsDataFileChunk implements DataFileChunk {

    private FSDataInputStream fileStream;

    public HdfsDataFileChunk(FileSystem fs, FileStatus dataFile) throws IOException {
        this.fileStream = fs.open(dataFile.getPath());
    }

    public int read(ByteBuffer buffer, long currentOffset) throws IOException {
        byte[] bufferByte = new byte[buffer.capacity()];
        this.fileStream.readFully(currentOffset, bufferByte);
        buffer.put(bufferByte);
        return buffer.capacity();
    }
}
