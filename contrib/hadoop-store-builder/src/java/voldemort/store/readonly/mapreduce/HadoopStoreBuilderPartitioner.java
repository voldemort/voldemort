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

package voldemort.store.readonly.mapreduce;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import voldemort.VoldemortException;
import voldemort.store.readonly.ReadOnlyUtils;
import voldemort.utils.ByteUtils;

/**
 * A Partitioner that splits data so that all data for the same nodeId, chunkId
 * combination ends up in the same reduce (and hence in the same store chunk)
 * 
 * 
 */
public class HadoopStoreBuilderPartitioner extends Partitioner<BytesWritable, BytesWritable>
        implements Configurable {

    private int numChunks;
    private Configuration conf;

    @Override
    public int getPartition(BytesWritable key, BytesWritable value, int numPartitions) {
        int partitionId = ByteUtils.readInt(value.getBytes(), 4);
        int chunkId = ReadOnlyUtils.chunk(key.getBytes(), numChunks);
        return (partitionId * numChunks + chunkId) % numPartitions;
    }

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
        this.numChunks = conf.getInt("num.chunks", -1);
        if(this.numChunks < 1)
            throw new VoldemortException("num.chunks not specified in the job conf.");
    }

}
