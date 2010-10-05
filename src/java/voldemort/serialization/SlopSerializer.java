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

import voldemort.client.protocol.pb.ProtoUtils;
import voldemort.store.slop.Slop;
import voldemort.utils.ByteArray;

/**
 * A Serializer for writing Slops
 * 
 * 
 */
public class SlopSerializer implements Serializer<Slop> {

    public byte[] toBytes(Slop slop) {
        ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
        DataOutputStream data = new DataOutputStream(byteOutput);
        try {
            VSlopProto.Slop.Builder builder = VSlopProto.Slop.newBuilder()
                                                             .setStore(slop.getStoreName())
                                                             .setOperation(slop.getOperation().toString())
                                                             .setKey(ProtoUtils.encodeBytes(slop.getKey()))
                                                             .setNodeId(slop.getNodeId())
                                                             .setArrived(slop.getArrived().getTime());

            if (slop.getValue() != null)
                builder.setValue(ProtoUtils.encodeBytes(new ByteArray(slop.getValue())));

            ProtoUtils.writeMessage(data, builder.build());

            return byteOutput.toByteArray();
        } catch(IOException e) {
            throw new SerializationException(e);
        }
    }

    public Slop toObject(byte[] bytes) {
        DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes));
        try {
            VSlopProto.Slop proto = ProtoUtils.readToBuilder(input, VSlopProto.Slop.newBuilder())
                                               .build();

            String storeName = proto.getStore();
            Slop.Operation op = Slop.Operation.valueOf(proto.getOperation());
            byte[] key = ProtoUtils.decodeBytes(proto.getKey()).get();
            byte[] value = null;

            if (proto.hasValue())
                value = ProtoUtils.decodeBytes(proto.getValue()).get();
            
            int nodeId = proto.getNodeId();
            Date arrived = new Date(proto.getArrived());
            
            return new Slop(storeName, op, key, value, nodeId, arrived);
        } catch(IOException e) {
            throw new SerializationException(e);
        }
    }

}
