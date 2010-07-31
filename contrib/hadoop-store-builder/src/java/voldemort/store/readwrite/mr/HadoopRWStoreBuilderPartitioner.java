/*
 * Copyright 2010 LinkedIn, Inc
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

package voldemort.store.readwrite.mr;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.Partitioner;

import voldemort.store.readonly.mr.AbstractStoreBuilderConfigurable;
import voldemort.utils.ByteUtils;

/**
 * A Partitioner that splits data so that we have reducers = partitions
 */
@SuppressWarnings("deprecation")
public class HadoopRWStoreBuilderPartitioner extends AbstractStoreBuilderConfigurable implements
        Partitioner<BytesWritable, BytesWritable> {

    public int getPartition(BytesWritable key, BytesWritable value, int numReduceTasks) {
        int nodeId = ByteUtils.readInt(key.get(), 0);
        int chunkId = ByteUtils.readInt(key.get(), 4);
        return (nodeId * getNumChunks() + chunkId) % numReduceTasks;
    }

}
