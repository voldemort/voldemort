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

package voldemort.store.readonly.mr.utils;

import voldemort.store.StoreDefinition;
import voldemort.store.readonly.ReadOnlyUtils;
import voldemort.utils.ByteUtils;

public class KeyValuePartitioner {

    public int getPartition(byte[] keyBytes,
                            byte[] valueBytes,
                            boolean saveKeys,
                            boolean reducerPerBucket,
                            StoreDefinition storeDef,
                            int numChunks,
                            int numReduceTasks) {
        int partitionId = ByteUtils.readInt(valueBytes, ByteUtils.SIZE_OF_INT);
        int chunkId = ReadOnlyUtils.chunk(keyBytes, numChunks);
        if(saveKeys) {
            int replicaType = (int) ByteUtils.readBytes(valueBytes,
                                                        2 * ByteUtils.SIZE_OF_INT,
                                                        ByteUtils.SIZE_OF_BYTE);
            if(reducerPerBucket) {
                return (partitionId * storeDef.getReplicationFactor() + replicaType)
                       % numReduceTasks;
            } else {
                return ((partitionId * storeDef.getReplicationFactor() * numChunks)
                        + (replicaType * numChunks) + chunkId)
                       % numReduceTasks;
            }
        } else {
            if(reducerPerBucket) {
                return partitionId % numReduceTasks;
            } else {
                return (partitionId * numChunks + chunkId) % numReduceTasks;
            }

        }
    }

}
