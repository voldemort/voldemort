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

package voldemort.store.readonly.mr.serialization;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.InputBuffer;
import org.apache.hadoop.io.RawComparator;

import voldemort.serialization.SerializationException;
import voldemort.serialization.json.JsonTypeSerializer;

/**
 * A hadoop RawComparator that deserializes first. Usefull for sorting JSON
 * objects
 * 
 * @author jay
 * 
 */
public class JsonDeserializerComparator implements RawComparator<BytesWritable>, Configurable {

    /**
     * Should be same as BytesWritable.Length
     */
    private int LENGTH_BYTES = 4;

    private Configuration config;
    private InputBuffer buffer = new InputBuffer();
    private DataInputStream dataInput = new DataInputStream(buffer);
    private JsonTypeSerializer serializer;

    public Configuration getConf() {
        return this.config;
    }

    public void setConf(Configuration config) {
        if(config.get("json.schema") == null)
            throw new IllegalArgumentException("No schema has been set!");
        this.serializer = new JsonTypeSerializer(config.get("json.schema"));
    }

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return compareBytes(b1,
                            s1 + LENGTH_BYTES,
                            l1 - LENGTH_BYTES,
                            b2,
                            s2 + LENGTH_BYTES,
                            l2 - LENGTH_BYTES);
    }

    public int compareBytes(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        if(serializer == null)
            throw new SerializationException("No serializer has been set!");
        try {
            buffer.reset(b1, s1, l1);
            Object key1 = serializer.toObject(dataInput);

            buffer.reset(b2, s2, l2);
            Object key2 = serializer.toObject(dataInput);

            if(key1 instanceof Comparable) {
                return this.compareSerializedObjects(key1, key2);
            } else {
                return customCompare(key1, key2, serializer);
            }
        } catch(IOException e) {
            throw new SerializationException(e);
        }
    }

    public int customCompare(Object key1, Object key2, JsonTypeSerializer serializer) {
        byte[] b1 = serializer.toBytes(key1);
        byte[] b2 = serializer.toBytes(key2);

        return BytesWritable.Comparator.compareBytes(b1, 0, b1.length, b2, 0, b2.length);
    }

    public int compare(BytesWritable o1, BytesWritable o2) {
        return this.compareBytes(o1.getBytes(), 0, o1.getLength(), o2.getBytes(), 0, o2.getLength());
    }

    public int compareSerializedObjects(Object o1, Object o2) {
        if(o1 == o2)
            return 0;
        else if(o1 == null)
            return -1;
        else if(o2 == null)
            return 1;
        else if(o1.getClass() != o2.getClass())
            throw new IllegalArgumentException("Attempt to compare two items of different classes: "
                                               + o1.getClass() + " and " + o2.getClass());
        else if(o1 instanceof Comparable)
            return ((Comparable) o1).compareTo(o2);

        throw new IllegalArgumentException("Incomparable object type!");
    }
}
