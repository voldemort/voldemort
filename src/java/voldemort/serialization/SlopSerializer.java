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

package voldemort.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Date;

import voldemort.store.slop.Slop;
import voldemort.utils.ByteUtils;

/**
 * A Serializer for writing Slops
 * 
 * @author jay
 * 
 */
public class SlopSerializer implements Serializer<Slop> {

    public byte[] toBytes(Slop slop) {
        ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
        DataOutputStream data = new DataOutputStream(byteOutput);
        try {
            data.writeUTF(slop.getStoreName());
            data.writeUTF(slop.getOperation().toString());
            data.writeInt(slop.getKey().length);
            data.write(slop.getKey());
            if(slop.getValue() == null) {
                data.writeInt(-1);
            } else {
                data.writeInt(slop.getValue().length);
                data.write(slop.getValue());
            }
            data.writeInt(slop.getNodeId());
            data.writeLong(slop.getArrived().getTime());
            return byteOutput.toByteArray();
        } catch(IOException e) {
            throw new SerializationException(e);
        }
    }

    public Slop toObject(byte[] bytes) {
        DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes));
        try {
            String storeName = input.readUTF();
            Slop.Operation op = Slop.Operation.valueOf(input.readUTF());
            int keySize = input.readInt();
            byte[] key = new byte[keySize];
            ByteUtils.read(input, key);
            int size = input.readInt();
            byte[] value = null;
            if(size >= 0) {
                value = new byte[size];
                ByteUtils.read(input, value);
            }
            int nodeId = input.readInt();
            Date arrived = new Date(input.readLong());
            return new Slop(storeName, op, key, value, nodeId, arrived);
        } catch(IOException e) {
            throw new SerializationException(e);
        }
    }

}
