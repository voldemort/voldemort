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

package voldemort.store.socket;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.serialization.VoldemortOpCode;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.store.UnreachableStoreException;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.Utils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * The client implementation of a socket store--tranlates each request into a
 * network operation to be handled by the socket server on the other side.
 * 
 * @author jay
 * 
 */
public class SocketStore implements Store<ByteArray, byte[]> {

    private static final Logger logger = Logger.getLogger(SocketStore.class);
    private final ErrorCodeMapper errorCodeMapper = new ErrorCodeMapper();

    private final String name;
    private final SocketPool pool;
    private final SocketDestination destination;

    public SocketStore(String name, String host, int port, SocketPool socketPool) {
        this.name = name;
        this.pool = socketPool;
        this.destination = new SocketDestination(Utils.notNull(host), port);
    }

    public void close() throws VoldemortException {
    // don't close the socket pool, it is shared
    }

    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        SocketAndStreams sands = pool.checkout(destination);
        try {
            DataOutputStream outputStream = sands.getOutputStream();
            outputStream.writeByte(VoldemortOpCode.DELETE_OP_CODE);
            outputStream.writeUTF(name);
            outputStream.writeInt(key.length());
            outputStream.write(key.get());
            VectorClock clock = (VectorClock) version;
            outputStream.writeShort(clock.sizeInBytes());
            outputStream.write(clock.toBytes());
            outputStream.flush();
            DataInputStream inputStream = sands.getInputStream();
            checkException(inputStream);
            return inputStream.readBoolean();
        } catch(IOException e) {
            close(sands.getSocket());
            throw new UnreachableStoreException("Failure in delete on " + destination + ": "
                                                + e.getMessage(), e);
        } finally {
            pool.checkin(destination, sands);
        }
    }

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        // TODO We can optimise this, but wait for protobuf protocol before
        // considering
        return StoreUtils.getAll(this, keys);
    }

    public List<Versioned<byte[]>> get(ByteArray key) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        SocketAndStreams sands = pool.checkout(destination);
        try {
            DataOutputStream outputStream = sands.getOutputStream();
            outputStream.writeByte(VoldemortOpCode.GET_OP_CODE);
            outputStream.writeUTF(name);
            outputStream.writeInt(key.length());
            outputStream.write(key.get());
            outputStream.flush();
            DataInputStream inputStream = sands.getInputStream();
            checkException(inputStream);
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
        } catch(IOException e) {
            close(sands.getSocket());
            throw new UnreachableStoreException("Failure in get on " + destination + ": "
                                                + e.getMessage(), e);
        } finally {
            pool.checkin(destination, sands);
        }
    }

    public void put(ByteArray key, Versioned<byte[]> value) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        SocketAndStreams sands = pool.checkout(destination);
        try {
            DataOutputStream outputStream = sands.getOutputStream();
            outputStream.writeByte(VoldemortOpCode.PUT_OP_CODE);
            outputStream.writeUTF(name);
            outputStream.writeInt(key.length());
            outputStream.write(key.get());
            VectorClock clock = (VectorClock) value.getVersion();
            outputStream.writeInt(value.getValue().length + clock.sizeInBytes());
            outputStream.write(clock.toBytes());
            outputStream.write(value.getValue());
            outputStream.flush();
            DataInputStream inputStream = sands.getInputStream();
            checkException(inputStream);
        } catch(IOException e) {
            close(sands.getSocket());
            throw new UnreachableStoreException("Failure in put on " + destination + ": "
                                                + e.getMessage(), e);
        } finally {
            pool.checkin(destination, sands);
        }
    }

    public Object getCapability(StoreCapabilityType capability) {
        if(StoreCapabilityType.SOCKET_POOL.equals(capability))
            return this.pool;
        else
            throw new NoSuchCapabilityException(capability, getName());
    }

    public String getName() {
        return name;
    }

    private void checkException(DataInputStream inputStream) throws IOException {
        short retCode = inputStream.readShort();
        if(retCode != 0) {
            String error = inputStream.readUTF();
            throw errorCodeMapper.getError(retCode, error);
        }
    }

    private void close(Socket socket) {
        try {
            socket.close();
        } catch(IOException e) {
            logger.warn("Failed to close socket");
        }
    }
}
