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

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.Partitioner;

import voldemort.store.readonly.ReadOnlyUtils;
import voldemort.utils.ByteUtils;

/**
 * A Partitioner that splits data so that all data for the same nodeId, chunkId
 * combination ends up in the same reduce
 * 
 * @author bbansal, jay
 * 
 */
public class HadoopStoreBuilderPartitioner extends HadoopStoreBuilderBase implements
        Partitioner<BytesWritable, BytesWritable> {

    public int getPartition(BytesWritable key, BytesWritable value, int numReduceTasks) {
        int nodeId = ByteUtils.readInt(value.get(), 0);
        int chunkId = ReadOnlyUtils.chunk(key.get(), getNumChunks());
        return (nodeId * getNumChunks() + chunkId) % numReduceTasks;
    }

}
