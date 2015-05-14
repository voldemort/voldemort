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

import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;

import voldemort.cluster.Node;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.store.compress.CompressionStrategy;
import voldemort.utils.ByteUtils;
import voldemort.utils.Pair;

public class MapperKeyValueWriter {

    public BytesWritable getOutputKey() {
        return outputKey;
    }

    public BytesWritable getOutputValue() {
        return outputVal;
    }

    BytesWritable outputKey;
    BytesWritable outputVal;

    public List<Pair<BytesWritable, BytesWritable>> map(ConsistentRoutingStrategy routingStrategy,
                                                        Serializer keySerializer,
                                                        Serializer valueSerializer,
                                                        CompressionStrategy valueCompressor,
                                                        CompressionStrategy keyCompressor,
                                                        SerializerDefinition keySerializerDefinition,
                                                        SerializerDefinition valueSerializerDefinition,
                                                        byte[] keyBytes,
                                                        byte[] valBytes,
                                                        boolean getSaveKeys,
                                                        MessageDigest md5er) throws IOException {

        List outputList = new ArrayList();
        // Compress key and values if required
        if(keySerializerDefinition.hasCompression()) {
            keyBytes = keyCompressor.deflate(keyBytes);
        }

        if(valueSerializerDefinition.hasCompression()) {
            valBytes = valueCompressor.deflate(valBytes);
        }

        // Get the output byte arrays ready to populate
        byte[] outputValue;

        // Leave initial offset for (a) node id (b) partition id
        // since they are written later
        int offsetTillNow = 2 * ByteUtils.SIZE_OF_INT;

        if(getSaveKeys) {

            // In order - 4 ( for node id ) + 4 ( partition id ) + 1 (
            // replica
            // type - primary | secondary | tertiary... ] + 4 ( key size )
            // size ) + 4 ( value size ) + key + value
            outputValue = new byte[valBytes.length + keyBytes.length + ByteUtils.SIZE_OF_BYTE + 4
                                   * ByteUtils.SIZE_OF_INT];

            // Write key length - leave byte for replica type
            offsetTillNow += ByteUtils.SIZE_OF_BYTE;
            ByteUtils.writeInt(outputValue, keyBytes.length, offsetTillNow);

            // Write value length
            offsetTillNow += ByteUtils.SIZE_OF_INT;
            ByteUtils.writeInt(outputValue, valBytes.length, offsetTillNow);

            // Write key
            offsetTillNow += ByteUtils.SIZE_OF_INT;
            System.arraycopy(keyBytes, 0, outputValue, offsetTillNow, keyBytes.length);

            // Write value
            offsetTillNow += keyBytes.length;
            System.arraycopy(valBytes, 0, outputValue, offsetTillNow, valBytes.length);

            // Generate MR key - upper 8 bytes of 16 byte md5
            outputKey = new BytesWritable(ByteUtils.copy(md5er.digest(keyBytes),
                                                         0,
                                                         2 * ByteUtils.SIZE_OF_INT));

        } else {

            // In order - 4 ( for node id ) + 4 ( partition id ) + value
            outputValue = new byte[valBytes.length + 2 * ByteUtils.SIZE_OF_INT];

            // Write value
            System.arraycopy(valBytes, 0, outputValue, offsetTillNow, valBytes.length);

            // Generate MR key - 16 byte md5
            outputKey = new BytesWritable(md5er.digest(keyBytes));

        }

        // Generate partition and node list this key is destined for
        List<Integer> partitionList = routingStrategy.getPartitionList(keyBytes);
        Node[] partitionToNode = routingStrategy.getPartitionToNode();

        for(int replicaType = 0; replicaType < partitionList.size(); replicaType++) {

            // Node id
            ByteUtils.writeInt(outputValue,
                               partitionToNode[partitionList.get(replicaType)].getId(),
                               0);

            if(getSaveKeys) {
                // Primary partition id
                ByteUtils.writeInt(outputValue, partitionList.get(0), ByteUtils.SIZE_OF_INT);

                // Replica type
                ByteUtils.writeBytes(outputValue,
                                     replicaType,
                                     2 * ByteUtils.SIZE_OF_INT,
                                     ByteUtils.SIZE_OF_BYTE);
            } else {
                // Partition id
                ByteUtils.writeInt(outputValue,
                                   partitionList.get(replicaType),
                                   ByteUtils.SIZE_OF_INT);
            }
            outputVal = new BytesWritable(outputValue);
            Pair<BytesWritable, BytesWritable> pair = Pair.create(outputKey, outputVal);
            outputList.add(pair);
        }

        return outputList;

    }
}
