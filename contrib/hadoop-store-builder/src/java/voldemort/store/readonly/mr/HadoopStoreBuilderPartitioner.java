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
 * combination ends up in the same reduce (and hence in the same store chunk)
 */
@SuppressWarnings("deprecation")
public class HadoopStoreBuilderPartitioner extends AbstractStoreBuilderConfigurable implements
        Partitioner<BytesWritable, BytesWritable> {

    public int getPartition(BytesWritable key, BytesWritable value, int numReduceTasks) {
        int partitionId = ByteUtils.readInt(value.get(), ByteUtils.SIZE_OF_INT);
        int chunkId = ReadOnlyUtils.chunk(key.get(), getNumChunks());
        if(getSaveKeys()) {
            int replicaType = (int) ByteUtils.readBytes(value.get(),
                                                        2 * ByteUtils.SIZE_OF_INT,
                                                        ByteUtils.SIZE_OF_BYTE);
            return ((partitionId * getStoreDef().getReplicationFactor() * getNumChunks())
                    + (replicaType * getNumChunks()) + chunkId)
                   % numReduceTasks;
        } else {
            return (partitionId * getNumChunks() + chunkId) % numReduceTasks;
        }
    }
}
