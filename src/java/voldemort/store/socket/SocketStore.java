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

import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormat;
import voldemort.client.protocol.RequestFormatFactory;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.store.UnreachableStoreException;
import voldemort.utils.ByteArray;
import voldemort.utils.Utils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * The client implementation of a socket store--translates each request into a
 * network operation to be handled by the socket server on the other side.
 * 
 * @author jay
 * 
 */
public class SocketStore implements Store<ByteArray, byte[]> {

    private static final Logger logger = Logger.getLogger(SocketStore.class);

    private final RequestFormatFactory requestFormatFactory = new RequestFormatFactory();

    private final String name;
    private final SocketPool pool;
    private final SocketDestination destination;
    private final RequestFormat requestFormat;
    private final boolean reroute;

    public SocketStore(String name, SocketDestination dest, SocketPool socketPool, boolean reroute) {
        this.name = Utils.notNull(name);
        this.pool = Utils.notNull(socketPool);
        this.destination = dest;
        this.requestFormat = requestFormatFactory.getRequestFormat(dest.getRequestFormatType());
        this.reroute = reroute;
    }

    public void close() throws VoldemortException {
    // don't close the socket pool, it is shared
    }

    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        SocketAndStreams sands = pool.checkout(destination);
        try {
            requestFormat.writeDeleteRequest(sands.getOutputStream(),
                                             name,
                                             key,
                                             (VectorClock) version,
                                             reroute);
            sands.getOutputStream().flush();
            return requestFormat.readDeleteResponse(sands.getInputStream());
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
        SocketAndStreams sands = pool.checkout(destination);
        try {
            requestFormat.writeGetAllRequest(sands.getOutputStream(), name, keys, reroute);
            sands.getOutputStream().flush();
            return requestFormat.readGetAllResponse(sands.getInputStream());
        } catch(IOException e) {
            close(sands.getSocket());
            throw new VoldemortException(e);
        } finally {
            pool.checkin(destination, sands);
        }
    }

    public List<Versioned<byte[]>> get(ByteArray key) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        SocketAndStreams sands = pool.checkout(destination);
        try {
            requestFormat.writeGetRequest(sands.getOutputStream(), name, key, reroute);
            sands.getOutputStream().flush();
            return requestFormat.readGetResponse(sands.getInputStream());
        } catch(IOException e) {
            close(sands.getSocket());
            throw new UnreachableStoreException("Failure in get on " + destination + ": "
                                                + e.getMessage(), e);
        } finally {
            pool.checkin(destination, sands);
        }
    }

    public void put(ByteArray key, Versioned<byte[]> versioned) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        SocketAndStreams sands = pool.checkout(destination);
        try {
            requestFormat.writePutRequest(sands.getOutputStream(),
                                          name,
                                          key,
                                          versioned.getValue(),
                                          (VectorClock) versioned.getVersion(),
                                          reroute);
            sands.getOutputStream().flush();
            requestFormat.readPutResponse(sands.getInputStream());
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

    private void close(Socket socket) {
        try {
            socket.close();
        } catch(IOException e) {
            logger.warn("Failed to close socket");
        }
    }

    public List<Version> getVersions(ByteArray key) {
        StoreUtils.assertValidKey(key);
        SocketAndStreams sands = pool.checkout(destination);
        try {
            requestFormat.writeGetVersionRequest(sands.getOutputStream(), name, key, reroute);
            sands.getOutputStream().flush();
            return requestFormat.readGetVersionResponse(sands.getInputStream());
        } catch(IOException e) {
            close(sands.getSocket());
            throw new UnreachableStoreException("Failure in getVersion on " + destination + ": "
                                                + e.getMessage(), e);
        } finally {
            pool.checkin(destination, sands);
        }
    }
}
