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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormat;
import voldemort.client.protocol.RequestFormatFactory;
import voldemort.server.RequestRoutingType;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.store.UnreachableStoreException;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.nonblockingstore.NonblockingStoreCallback;
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
import voldemort.utils.Time;
import voldemort.utils.Utils;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * The client implementation of a socket store--translates each request into a
 * network operation to be handled by the socket server on the other side.
 * 
 * <p/>
 * 
 * SocketStore handles both <i>blocking</i> and <i>non-blocking</i> styles of
 * requesting. For non-blocking requests, SocketStore checks out a
 * {@link ClientRequestExecutor} instance from the
 * {@link ClientRequestExecutorPool pool} and adds an appropriate
 * {@link ClientRequest request} to be processed by the NIO thread.
 */
public class SocketStore implements Store<ByteArray, byte[], byte[]>, NonblockingStore {

    private final RequestFormatFactory requestFormatFactory = new RequestFormatFactory();

    private final String storeName;
    private final long timeoutMs;
    private final ClientRequestExecutorPool pool;
    private final SocketDestination destination;
    private final RequestFormat requestFormat;
    private final RequestRoutingType requestRoutingType;
    private final Logger logger = Logger.getLogger(SocketStore.class);

    public SocketStore(String storeName,
                       long timeoutMs,
                       SocketDestination dest,
                       ClientRequestExecutorPool pool,
                       RequestRoutingType requestRoutingType) {
        this.storeName = Utils.notNull(storeName);
        this.timeoutMs = timeoutMs;
        this.pool = Utils.notNull(pool);
        this.destination = dest;
        this.requestFormat = requestFormatFactory.getRequestFormat(dest.getRequestFormatType());
        this.requestRoutingType = requestRoutingType;
    }

    public void submitDeleteRequest(ByteArray key,
                                    Version version,
                                    NonblockingStoreCallback callback,
                                    long timeoutMs) {
        StoreUtils.assertValidKey(key);
        DeleteClientRequest clientRequest = new DeleteClientRequest(storeName,
                                                                    requestFormat,
                                                                    requestRoutingType,
                                                                    key,
                                                                    version);
        requestAsync(clientRequest, callback, timeoutMs, "delete");
    }

    public void submitGetRequest(ByteArray key,
                                 byte[] transforms,
                                 NonblockingStoreCallback callback,
                                 long timeoutMs) {
        StoreUtils.assertValidKey(key);
        GetClientRequest clientRequest = new GetClientRequest(storeName,
                                                              requestFormat,
                                                              requestRoutingType,
                                                              key,
                                                              transforms);
        requestAsync(clientRequest, callback, timeoutMs, "get");
    }

    public void submitGetAllRequest(Iterable<ByteArray> keys,
                                    Map<ByteArray, byte[]> transforms,
                                    NonblockingStoreCallback callback,
                                    long timeoutMs) {
        StoreUtils.assertValidKeys(keys);
        GetAllClientRequest clientRequest = new GetAllClientRequest(storeName,
                                                                    requestFormat,
                                                                    requestRoutingType,
                                                                    keys,
                                                                    transforms);
        requestAsync(clientRequest, callback, timeoutMs, "get all");
    }

    public void submitGetVersionsRequest(ByteArray key,
                                         NonblockingStoreCallback callback,
                                         long timeoutMs) {
        StoreUtils.assertValidKey(key);
        GetVersionsClientRequest clientRequest = new GetVersionsClientRequest(storeName,
                                                                              requestFormat,
                                                                              requestRoutingType,
                                                                              key);
        requestAsync(clientRequest, callback, timeoutMs, "get versions");
    }

    public void submitPutRequest(ByteArray key,
                                 Versioned<byte[]> value,
                                 byte[] transforms,
                                 NonblockingStoreCallback callback,
                                 long timeoutMs) {
        StoreUtils.assertValidKey(key);
        PutClientRequest clientRequest = new PutClientRequest(storeName,
                                                              requestFormat,
                                                              requestRoutingType,
                                                              key,
                                                              value,
                                                              transforms);
        requestAsync(clientRequest, callback, timeoutMs, "put");
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

    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        GetClientRequest clientRequest = new GetClientRequest(storeName,
                                                              requestFormat,
                                                              requestRoutingType,
                                                              key,
                                                              transforms);
        return request(clientRequest, "get");
    }

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        GetAllClientRequest clientRequest = new GetAllClientRequest(storeName,
                                                                    requestFormat,
                                                                    requestRoutingType,
                                                                    keys,
                                                                    transforms);
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

    public void put(ByteArray key, Versioned<byte[]> versioned, byte[] transforms)
            throws VoldemortException {
        StoreUtils.assertValidKey(key);
        PutClientRequest clientRequest = new PutClientRequest(storeName,
                                                              requestFormat,
                                                              requestRoutingType,
                                                              key,
                                                              versioned,
                                                              transforms);
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
            BlockingClientRequest<T> blockingClientRequest = new BlockingClientRequest<T>(delegate,
                                                                                          timeoutMs);
            clientRequestExecutor.addClientRequest(blockingClientRequest, timeoutMs);
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

    private <T> void requestAsync(ClientRequest<T> delegate,
                                  NonblockingStoreCallback callback,
                                  long timeoutMs,
                                  String operationName) {
        ClientRequestExecutor clientRequestExecutor = null;

        try {
            clientRequestExecutor = pool.checkout(destination);
        } catch(Exception e) {
            // If we can't check out a socket from the pool, we'll usually get
            // either an IOException (subclass) or an UnreachableStoreException
            // error. However, in the case of asynchronous calls, we want the
            // error to be reported via our callback, not returned to the caller
            // directly.
            if(!(e instanceof UnreachableStoreException))
                e = new UnreachableStoreException("Failure in " + operationName + ": "
                                                  + e.getMessage(), e);

            try {
                callback.requestComplete(e, 0);
            } catch(Exception ex) {
                if(logger.isEnabledFor(Level.WARN))
                    logger.warn(ex, ex);
            }

            return;
        }

        NonblockingStoreCallbackClientRequest<T> clientRequest = new NonblockingStoreCallbackClientRequest<T>(delegate,
                                                                                                              clientRequestExecutor,
                                                                                                              callback);
        clientRequestExecutor.addClientRequest(clientRequest, timeoutMs);
    }

    private class NonblockingStoreCallbackClientRequest<T> implements ClientRequest<T> {

        private final ClientRequest<T> clientRequest;

        private final ClientRequestExecutor clientRequestExecutor;

        private final NonblockingStoreCallback callback;

        private final long startNs;

        private volatile boolean isComplete;

        public NonblockingStoreCallbackClientRequest(ClientRequest<T> clientRequest,
                                                     ClientRequestExecutor clientRequestExecutor,
                                                     NonblockingStoreCallback callback) {
            this.clientRequest = clientRequest;
            this.clientRequestExecutor = clientRequestExecutor;
            this.callback = callback;
            this.startNs = System.nanoTime();
        }

        public void complete() {
            try {
                clientRequest.complete();
                Object result = clientRequest.getResult();

                if(callback != null) {
                    try {
                        callback.requestComplete(result, (System.nanoTime() - startNs)
                                                         / Time.NS_PER_MS);
                    } catch(Exception e) {
                        if(logger.isEnabledFor(Level.WARN))
                            logger.warn(e, e);
                    }
                }
            } catch(Exception e) {
                if(callback != null) {
                    try {
                        callback.requestComplete(e, (System.nanoTime() - startNs) / Time.NS_PER_MS);
                    } catch(Exception ex) {
                        if(logger.isEnabledFor(Level.WARN))
                            logger.warn(ex, ex);
                    }
                }
            } finally {
                pool.checkin(destination, clientRequestExecutor);
                isComplete = true;
            }
        }

        public boolean isComplete() {
            return isComplete;
        }

        public boolean formatRequest(DataOutputStream outputStream) {
            return clientRequest.formatRequest(outputStream);
        }

        public T getResult() throws VoldemortException, IOException {
            return clientRequest.getResult();
        }

        public boolean isCompleteResponse(ByteBuffer buffer) {
            return clientRequest.isCompleteResponse(buffer);
        }

        public void parseResponse(DataInputStream inputStream) {
            clientRequest.parseResponse(inputStream);
        }

    }

}
