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

import voldemort.common.VoldemortOpCode;
import voldemort.utils.ByteUtils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public final class VoldemortOperation {

    private final byte opCode;
    private final String key;
    private final byte[] value;
    private final VectorClock version;

    private VoldemortOperation(byte opCode, String key, byte[] value, VectorClock version) {
        this.opCode = opCode;
        this.key = key;
        this.value = value;
        this.version = version;
    }

    public VoldemortOperation(byte[] bytes) {
        if(bytes == null || bytes.length <= 1)
            throw new SerializationException("Not enough bytes to serialize");
        DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(bytes));
        try {
            this.opCode = inputStream.readByte();
            switch(opCode) {
                case VoldemortOpCode.GET_OP_CODE:
                    this.version = null;
                    this.key = inputStream.readUTF();
                    this.value = null;
                    break;
                case VoldemortOpCode.PUT_OP_CODE:
                    this.version = new VectorClock(bytes, 1);
                    this.key = inputStream.readUTF();
                    int valueSize = inputStream.readInt();
                    this.value = new byte[valueSize];
                    ByteUtils.read(inputStream, this.value);
                    break;
                case VoldemortOpCode.DELETE_OP_CODE:
                    this.version = new VectorClock(bytes, 1);
                    this.key = inputStream.readUTF();
                    this.value = null;
                    break;
                default:
                    throw new SerializationException("Unknown opcode: " + bytes[0]);
            }
        } catch(IOException e) {
            throw new SerializationException(e);
        }
    }

    public byte[] getBytes() {
        try {
            ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(byteOutput);
            output.writeByte(opCode);
            if(opCode != VoldemortOpCode.GET_OP_CODE)
                output.write(version.toBytes());
            output.writeUTF(key);
            if(opCode == VoldemortOpCode.PUT_OP_CODE) {
                output.writeInt(value.length);
                output.write(value);
            }
            return byteOutput.toByteArray();
        } catch(IOException e) {
            throw new SerializationException(e);
        }
    }

    public VoldemortOperation makeGetOperation(String key) {
        return new VoldemortOperation(VoldemortOpCode.GET_OP_CODE, key, null, null);
    }

    public VoldemortOperation makePutOperation(String key, Versioned<byte[]> versioned) {
        return new VoldemortOperation(VoldemortOpCode.PUT_OP_CODE,
                                      key,
                                      versioned.getValue(),
                                      (VectorClock) versioned.getVersion());
    }

    public VoldemortOperation makeDeleteOperation(String key, Version version) {
        return new VoldemortOperation(VoldemortOpCode.DELETE_OP_CODE,
                                      key,
                                      null,
                                      (VectorClock) version);
    }

}
