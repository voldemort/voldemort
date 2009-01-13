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

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.serialization.VoldemortOpCode;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.Store;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteUtils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.base.Objects;

/**
 * The client implementation of a socket store--tranlates each request into a
 * network operation to be handled by the socket server on the other side.
 * 
 * @author jay
 * 
 */
public class SocketStore implements Store<byte[], byte[]> {

    private static final Logger logger = Logger.getLogger(SocketStore.class);
    private final ErrorCodeMapper errorCodeMapper = new ErrorCodeMapper();

    private final String name;
    private final SocketPool pool;
    private final SocketDestination destination;

    public SocketStore(String name, String host, int port, SocketPool socketPool) {
        this.name = name;
        this.pool = socketPool;
        this.destination = new SocketDestination(Objects.nonNull(host), port);
    }

    public void close() throws VoldemortException {
    // don't close the socket pool, it is shared
    }

    public boolean delete(byte[] key, Version version) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        SocketAndStreams sands = (SocketAndStreams) pool.checkout(destination);
        try {
            DataOutputStream outputStream = sands.getOutputStream();
            outputStream.writeByte(VoldemortOpCode.DELETE_OP_CODE);
            outputStream.writeUTF(name);
            outputStream.writeInt(key.length);
            outputStream.write(key);
            VectorClock clock = (VectorClock) version;
            outputStream.writeShort(clock.sizeInBytes());
            outputStream.write(clock.toBytes());
            outputStream.flush();
            DataInputStream inputStream = sands.getInputStream();
            checkException(inputStream);
            return inputStream.readBoolean();
        } catch(IOException e) {
            close(sands.getSocket());
            throw new VoldemortException(e);
        } finally {
            pool.checkin(destination, sands);
        }
    }

    public List<Versioned<byte[]>> get(byte[] key) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        SocketAndStreams sands = (SocketAndStreams) pool.checkout(destination);
        try {
            DataOutputStream outputStream = sands.getOutputStream();
            outputStream.writeByte(VoldemortOpCode.GET_OP_CODE);
            outputStream.writeUTF(name);
            outputStream.writeInt(key.length);
            outputStream.write(key);
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
            throw new VoldemortException(e);
        } finally {
            pool.checkin(destination, sands);
        }
    }

    public void put(byte[] key, Versioned<byte[]> value) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        SocketAndStreams sands = (SocketAndStreams) pool.checkout(destination);
        try {
            DataOutputStream outputStream = sands.getOutputStream();
            outputStream.writeByte(VoldemortOpCode.PUT_OP_CODE);
            outputStream.writeUTF(name);
            outputStream.writeInt(key.length);
            outputStream.write(key);
            VectorClock clock = (VectorClock) value.getVersion();
            outputStream.writeInt(value.getValue().length + clock.sizeInBytes());
            outputStream.write(clock.toBytes());
            outputStream.write(value.getValue());
            outputStream.flush();
            DataInputStream inputStream = sands.getInputStream();
            checkException(inputStream);
        } catch(IOException e) {
            close(sands.getSocket());
            throw new VoldemortException(e);
        } finally {
            pool.checkin(destination, sands);
        }
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
