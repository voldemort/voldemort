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

package voldemort.store.slop;

import java.util.Arrays;
import java.util.Date;

import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.Utils;

import com.google.common.base.Objects;

/**
 * Represents an undelivered operation to a store.
 * 
 * 
 */
public class Slop {

    private static final byte[] spacer = new byte[] { (byte) 0 };

    public enum Operation {
        PUT((byte) 0),
        DELETE((byte) 1);

        private final byte opCode;

        private Operation(byte opCode) {
            this.opCode = opCode;
        }

        public byte getOpCode() {
            return opCode;
        }
    }

    final private ByteArray key;
    final private byte[] value;
    final private byte[] transforms;
    final private String storeName;
    final private int nodeId;
    final private Date arrived;
    final private Operation operation;

    public Slop(String storeName,
                Operation operation,
                byte[] key,
                byte[] value,
                int nodeId,
                Date arrived) {
        this(storeName, operation, new ByteArray(key), value, null, nodeId, arrived);
    }

    public Slop(String storeName,
                Operation operation,
                ByteArray key,
                byte[] value,
                byte[] transforms,
                int nodeId,
                Date arrived) {
        this.operation = Utils.notNull(operation);
        this.storeName = Utils.notNull(storeName);
        this.key = Utils.notNull(key);
        this.value = value;
        this.transforms = transforms;
        this.nodeId = nodeId;
        this.arrived = Utils.notNull(arrived);
    }

    public ByteArray getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public byte[] getTransforms() {
        return transforms;
    }

    public int getNodeId() {
        return this.nodeId;
    }

    public Date getArrived() {
        return arrived;
    }

    public Operation getOperation() {
        return operation;
    }

    public String getStoreName() {
        return storeName;
    }

    public ByteArray makeKey() {
        byte[] storeName = ByteUtils.getBytes(getStoreName(), "UTF-8");
        byte[] opCode = new byte[] { operation.getOpCode() };
        byte[] nodeIdBytes = new byte[ByteUtils.SIZE_OF_INT];
        ByteUtils.writeInt(nodeIdBytes, nodeId, 0);
        return new ByteArray(ByteUtils.cat(opCode,
                                           spacer,
                                           storeName,
                                           spacer,
                                           nodeIdBytes,
                                           spacer,
                                           key.get()));
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null)
            return false;
        else if(!obj.getClass().equals(Slop.class))
            return false;

        Slop slop = (Slop) obj;

        return operation == slop.getOperation() && Objects.equal(storeName, getStoreName())
               && key.equals(slop.getKey()) && Utils.deepEquals(value, slop.getValue())
               && Utils.deepEquals(transforms, slop.getTransforms()) && nodeId == slop.getNodeId()
               && Objects.equal(arrived, slop.getArrived());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(storeName, operation, nodeId, arrived) + key.hashCode()
               + Arrays.hashCode(value) + Arrays.hashCode(transforms);
    }

    @Override
    public String toString() {
        return "Slop(storeName = " + storeName + ", operation = " + operation + ", key = " + key
               + ", value = " + Arrays.toString(value) + ", nodeId = " + nodeId + ", arrived = " + arrived + ")";
    }

}
