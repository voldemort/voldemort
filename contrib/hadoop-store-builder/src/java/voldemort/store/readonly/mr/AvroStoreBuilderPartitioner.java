package voldemort.store.readonly.mr;

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

import java.nio.ByteBuffer;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.mapred.Partitioner;

/**
 * A Partitioner that splits data so that all data for the same nodeId, chunkId
 * combination ends up in the same reduce (and hence in the same store chunk)
 */
public class AvroStoreBuilderPartitioner
        extends AbstractStoreBuilderConfigurable
        implements Partitioner<AvroKey<ByteBuffer>, AvroValue<ByteBuffer>> {

    @Override
    public int getPartition(AvroKey<ByteBuffer> key, AvroValue<ByteBuffer> value, int numReduceTasks) {

        byte[] keyBytes = null, valueBytes;

        keyBytes = new byte[key.datum().remaining()];
        key.datum().get(keyBytes);

        valueBytes = new byte[value.datum().remaining()];
        value.datum().get(valueBytes);

        ByteBuffer keyBuffer = null, valueBuffer = null;

        keyBuffer = ByteBuffer.allocate(keyBytes.length);
        keyBuffer.put(keyBytes);
        keyBuffer.rewind();

        valueBuffer = ByteBuffer.allocate(valueBytes.length);
        valueBuffer.put(valueBytes);
        valueBuffer.rewind();

        key.datum(keyBuffer);
        value.datum(valueBuffer);

        return getPartition(keyBytes, valueBytes, numReduceTasks);
    }
}
