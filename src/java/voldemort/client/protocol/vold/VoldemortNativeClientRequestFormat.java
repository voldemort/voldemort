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

package voldemort.client.protocol.vold;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import voldemort.client.protocol.RequestFormat;
import voldemort.serialization.VoldemortOpCode;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * The {@link voldemort.client.protocol.RequestFormat} for a low-overhead custom
 * binary protocol
 * 
 * @author jay
 * 
 */
public class VoldemortNativeClientRequestFormat implements RequestFormat {

    private final ErrorCodeMapper mapper;

    public VoldemortNativeClientRequestFormat() {
        this.mapper = new ErrorCodeMapper();
    }

    public void writeDeleteRequest(DataOutputStream outputStream,
                                   String storeName,
                                   ByteArray key,
                                   VectorClock version,
                                   boolean shouldReroute) throws IOException {
        StoreUtils.assertValidKey(key);
        outputStream.writeByte(VoldemortOpCode.DELETE_OP_CODE);
        outputStream.writeUTF(storeName);
        outputStream.writeBoolean(shouldReroute);
        outputStream.writeInt(key.length());
        outputStream.write(key.get());
        VectorClock clock = version;
        outputStream.writeShort(clock.sizeInBytes());
        outputStream.write(clock.toBytes());
    }

    public boolean readDeleteResponse(DataInputStream inputStream) throws IOException {
        checkException(inputStream);
        return inputStream.readBoolean();
    }

    public void writeGetRequest(DataOutputStream outputStream,
                                String storeName,
                                ByteArray key,
                                boolean shouldReroute) throws IOException {
        StoreUtils.assertValidKey(key);
        outputStream.writeByte(VoldemortOpCode.GET_OP_CODE);
        outputStream.writeUTF(storeName);
        outputStream.writeBoolean(shouldReroute);
        outputStream.writeInt(key.length());
        outputStream.write(key.get());
    }

    public List<Versioned<byte[]>> readGetResponse(DataInputStream inputStream) throws IOException {
        checkException(inputStream);
        return readResults(inputStream);
    }

    private List<Versioned<byte[]>> readResults(DataInputStream inputStream) throws IOException {
        int resultSize = inputStream.readInt();
        List<Versioned<byte[]>> results = new ArrayList<Versioned<byte[]>>(resultSize);
        for(int i = 0; i < resultSize; i++) {
            int valueSize = inputStream.readInt();
            byte[] bytes = new byte[valueSize];
            ByteUtils.read(inputStream, bytes);
            VectorClock clock = new VectorClock(bytes);
            results.add(new Versioned<byte[]>(ByteUtils.copy(bytes,
                                                             clock.sizeInBytes(),
                                                             bytes.length), clock));
        }
        return results;
    }

    public void writeGetAllRequest(DataOutputStream output,
                                   String storeName,
                                   Iterable<ByteArray> keys,
                                   boolean shouldReroute) throws IOException {
        StoreUtils.assertValidKeys(keys);
        output.writeByte(VoldemortOpCode.GET_ALL_OP_CODE);
        output.writeUTF(storeName);
        output.writeBoolean(shouldReroute);
        // write out keys
        List<ByteArray> l = new ArrayList<ByteArray>();
        for(ByteArray key: keys)
            l.add(key);
        output.writeInt(l.size());
        for(ByteArray key: keys) {
            output.writeInt(key.length());
            output.write(key.get());
        }
    }

    public Map<ByteArray, List<Versioned<byte[]>>> readGetAllResponse(DataInputStream stream)
            throws IOException {
        checkException(stream);
        int numResults = stream.readInt();
        Map<ByteArray, List<Versioned<byte[]>>> results = new HashMap<ByteArray, List<Versioned<byte[]>>>(numResults);
        for(int i = 0; i < numResults; i++) {
            int keySize = stream.readInt();
            byte[] key = new byte[keySize];
            stream.readFully(key);
            results.put(new ByteArray(key), readResults(stream));
        }
        return results;
    }

    public void writePutRequest(DataOutputStream outputStream,
                                String storeName,
                                ByteArray key,
                                byte[] value,
                                VectorClock version,
                                boolean shouldReroute) throws IOException {
        StoreUtils.assertValidKey(key);
        outputStream.writeByte(VoldemortOpCode.PUT_OP_CODE);
        outputStream.writeUTF(storeName);
        outputStream.writeBoolean(shouldReroute);
        outputStream.writeInt(key.length());
        outputStream.write(key.get());
        outputStream.writeInt(value.length + version.sizeInBytes());
        outputStream.write(version.toBytes());
        outputStream.write(value);
    }

    public void readPutResponse(DataInputStream inputStream) throws IOException {
        checkException(inputStream);
    }

    /*
     * If there is an exception, throw it
     */
    private void checkException(DataInputStream inputStream) throws IOException {
        short retCode = inputStream.readShort();
        if(retCode != 0) {
            String error = inputStream.readUTF();
            throw mapper.getError(retCode, error);
        }
    }

    public List<Version> readGetVersionResponse(DataInputStream stream) throws IOException {
        checkException(stream);
        int resultSize = stream.readInt();
        List<Version> results = new ArrayList<Version>(resultSize);
        for(int i = 0; i < resultSize; i++) {
            int versionSize = stream.readInt();
            byte[] bytes = new byte[versionSize];
            ByteUtils.read(stream, bytes);
            VectorClock clock = new VectorClock(bytes);
            results.add(clock);
        }
        return results;
    }

    public void writeGetVersionRequest(DataOutputStream output,
                                       String storeName,
                                       ByteArray key,
                                       boolean shouldReroute) throws IOException {
        StoreUtils.assertValidKey(key);
        output.writeByte(VoldemortOpCode.GET_VERSION_OP_CODE);
        output.writeUTF(storeName);
        output.writeBoolean(shouldReroute);
        output.writeInt(key.length());
        output.write(key.get());
    }
}
