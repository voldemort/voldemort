/*
 * Copyright 2008-2010 LinkedIn, Inc
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
import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormat;
import voldemort.client.protocol.RequestFormatFactory;
import voldemort.server.RequestRoutingType;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.store.UnreachableStoreException;
import voldemort.store.socket.clientrequest.BlockingClientRequest;
import voldemort.store.socket.clientrequest.ClientRequest;
import voldemort.store.socket.clientrequest.ClientRequestExecutor;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.store.socket.clientrequest.DeleteClientRequest;
import voldemort.store.socket.clientrequest.GetAllClientRequest;
import voldemort.store.socket.clientrequest.GetClientRequest;
import voldemort.store.socket.clientrequest.GetVersionsClientRequest;
import voldemort.store.socket.clientrequest.PutClientRequest;
import voldemort.utils.ByteArray;
import voldemort.utils.Utils;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * The client implementation of a socket store--translates each request into a
 * network operation to be handled by the socket server on the other side.
 * 
 * 
 */
public class SocketStore implements Store<ByteArray, byte[]> {

    private final RequestFormatFactory requestFormatFactory = new RequestFormatFactory();

    private final String storeName;
    private final ClientRequestExecutorPool pool;
    private final SocketDestination destination;
    private final RequestFormat requestFormat;
    private final RequestRoutingType requestRoutingType;

    public SocketStore(String storeName,
                       SocketDestination dest,
                       ClientRequestExecutorPool pool,
                       RequestRoutingType requestRoutingType) {
        this.storeName = Utils.notNull(storeName);
        this.pool = Utils.notNull(pool);
        this.destination = dest;
        this.requestFormat = requestFormatFactory.getRequestFormat(dest.getRequestFormatType());
        this.requestRoutingType = requestRoutingType;
    }

    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        DeleteClientRequest clientRequest = new DeleteClientRequest(storeName,
                                                                    requestFormat,
                                                                    requestRoutingType,
                                                                    key,
                                                                    version);
        return request(clientRequest, "delete");
    }

    public List<Versioned<byte[]>> get(ByteArray key) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        GetClientRequest clientRequest = new GetClientRequest(storeName,
                                                              requestFormat,
                                                              requestRoutingType,
                                                              key);
        return request(clientRequest, "get");
    }

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        GetAllClientRequest clientRequest = new GetAllClientRequest(storeName,
                                                                    requestFormat,
                                                                    requestRoutingType,
                                                                    keys);
        return request(clientRequest, "getAll");
    }

    public List<Version> getVersions(ByteArray key) {
        StoreUtils.assertValidKey(key);
        GetVersionsClientRequest clientRequest = new GetVersionsClientRequest(storeName,
                                                                              requestFormat,
                                                                              requestRoutingType,
                                                                              key);
        return request(clientRequest, "getVersions");
    }

    public void put(ByteArray key, Versioned<byte[]> versioned) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        PutClientRequest clientRequest = new PutClientRequest(storeName,
                                                              requestFormat,
                                                              requestRoutingType,
                                                              key,
                                                              versioned);
        request(clientRequest, "put");
    }

    public Object getCapability(StoreCapabilityType capability) {
        if(StoreCapabilityType.SOCKET_POOL.equals(capability))
            return this.pool;
        else
            throw new NoSuchCapabilityException(capability, getName());
    }

    public String getName() {
        return storeName;
    }

    public void close() throws VoldemortException {
    // don't close the socket pool, it is shared
    }

    /**
     * This method handles submitting and then waiting for the request from the
     * server. It uses the ClientRequest API to actually write the request and
     * then read back the response. This implementation will block for a
     * response from the server.
     * 
     * @param <T> Return type
     * 
     * @param clientRequest ClientRequest implementation used to write the
     *        request and read the response
     * @param operationName Simple string representing the type of request
     * 
     * @return Data returned by the individual requests
     */

    private <T> T request(ClientRequest<T> delegate, String operationName) {
        ClientRequestExecutor clientRequestExecutor = pool.checkout(destination);

        try {
            BlockingClientRequest<T> blockingClientRequest = new BlockingClientRequest<T>(delegate);
            clientRequestExecutor.addClientRequest(blockingClientRequest);
            blockingClientRequest.await();
            return blockingClientRequest.getResult();
        } catch(InterruptedException e) {
            throw new UnreachableStoreException("Failure in " + operationName + " on "
                                                + destination + ": " + e.getMessage(), e);
        } catch(IOException e) {
            clientRequestExecutor.close();
            throw new UnreachableStoreException("Failure in " + operationName + " on "
                                                + destination + ": " + e.getMessage(), e);
        } finally {
            pool.checkin(destination, clientRequestExecutor);
        }
    }

}
