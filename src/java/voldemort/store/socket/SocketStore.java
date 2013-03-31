/*
 * Copyright 2008-2012 LinkedIn, Inc
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

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormat;
import voldemort.client.protocol.RequestFormatFactory;
import voldemort.server.RequestRoutingType;
import voldemort.store.AbstractStore;
import voldemort.store.NoSuchCapabilityException;
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
public class SocketStore extends AbstractStore<ByteArray, byte[], byte[]> implements
        NonblockingStore {

    private final RequestFormatFactory requestFormatFactory = new RequestFormatFactory();

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
        super(storeName);
        this.timeoutMs = timeoutMs;
        this.pool = Utils.notNull(pool);
        this.destination = dest;
        this.requestFormat = requestFormatFactory.getRequestFormat(dest.getRequestFormatType());
        this.requestRoutingType = requestRoutingType;
    }

    @Override
    public void submitDeleteRequest(ByteArray key,
                                    Version version,
                                    NonblockingStoreCallback callback,
                                    long timeoutMs) {
        StoreUtils.assertValidKey(key);
        DeleteClientRequest clientRequest = new DeleteClientRequest(getName(),
                                                                    requestFormat,
                                                                    requestRoutingType,
                                                                    key,
                                                                    version);
        if(logger.isDebugEnabled())
            logger.debug("DELETE keyRef: " + System.identityHashCode(key) + " requestRef: "
                         + System.identityHashCode(clientRequest));
        requestAsync(clientRequest, callback, timeoutMs, "delete");
    }

    @Override
    public void submitGetRequest(ByteArray key,
                                 byte[] transforms,
                                 NonblockingStoreCallback callback,
                                 long timeoutMs) {
        StoreUtils.assertValidKey(key);
        GetClientRequest clientRequest = new GetClientRequest(getName(),
                                                              requestFormat,
                                                              requestRoutingType,
                                                              key,
                                                              transforms);
        if(logger.isDebugEnabled())
            logger.debug("GET keyRef: " + System.identityHashCode(key) + " requestRef: "
                         + System.identityHashCode(clientRequest));
        requestAsync(clientRequest, callback, timeoutMs, "get");
    }

    @Override
    public void submitGetAllRequest(Iterable<ByteArray> keys,
                                    Map<ByteArray, byte[]> transforms,
                                    NonblockingStoreCallback callback,
                                    long timeoutMs) {
        StoreUtils.assertValidKeys(keys);
        GetAllClientRequest clientRequest = new GetAllClientRequest(getName(),
                                                                    requestFormat,
                                                                    requestRoutingType,
                                                                    keys,
                                                                    transforms);
        if(logger.isDebugEnabled())
            logger.debug("GETALL keyRef: " + System.identityHashCode(keys) + " requestRef: "
                         + System.identityHashCode(clientRequest));
        requestAsync(clientRequest, callback, timeoutMs, "get all");
    }

    @Override
    public void submitGetVersionsRequest(ByteArray key,
                                         NonblockingStoreCallback callback,
                                         long timeoutMs) {
        StoreUtils.assertValidKey(key);
        GetVersionsClientRequest clientRequest = new GetVersionsClientRequest(getName(),
                                                                              requestFormat,
                                                                              requestRoutingType,
                                                                              key);
        if(logger.isDebugEnabled())
            logger.debug("GETVERSIONS keyRef: " + System.identityHashCode(key) + " requestRef: "
                         + System.identityHashCode(clientRequest));
        requestAsync(clientRequest, callback, timeoutMs, "get versions");
    }

    @Override
    public void submitPutRequest(ByteArray key,
                                 Versioned<byte[]> value,
                                 byte[] transforms,
                                 NonblockingStoreCallback callback,
                                 long timeoutMs) {
        StoreUtils.assertValidKey(key);
        PutClientRequest clientRequest = new PutClientRequest(getName(),
                                                              requestFormat,
                                                              requestRoutingType,
                                                              key,
                                                              value,
                                                              transforms);
        if(logger.isDebugEnabled())
            logger.debug("PUT keyRef: " + System.identityHashCode(key) + " requestRef: "
                         + System.identityHashCode(clientRequest));
        requestAsync(clientRequest, callback, timeoutMs, "put");
    }

    @Override
    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        DeleteClientRequest clientRequest = new DeleteClientRequest(getName(),
                                                                    requestFormat,
                                                                    requestRoutingType,
                                                                    key,
                                                                    version);
        if(logger.isDebugEnabled())
            logger.debug("DELETE keyRef: " + System.identityHashCode(key) + " requestRef: "
                         + System.identityHashCode(clientRequest));
        return request(clientRequest, "delete");
    }

    @Override
    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        GetClientRequest clientRequest = new GetClientRequest(getName(),
                                                              requestFormat,
                                                              requestRoutingType,
                                                              key,
                                                              transforms);
        if(logger.isDebugEnabled())
            logger.debug("GET keyRef: " + System.identityHashCode(key) + " requestRef: "
                         + System.identityHashCode(clientRequest));
        return request(clientRequest, "get");
    }

    @Override
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        GetAllClientRequest clientRequest = new GetAllClientRequest(getName(),
                                                                    requestFormat,
                                                                    requestRoutingType,
                                                                    keys,
                                                                    transforms);
        if(logger.isDebugEnabled())
            logger.debug("GETALL keyRef: " + System.identityHashCode(keys) + " requestRef: "
                         + System.identityHashCode(clientRequest));
        return request(clientRequest, "getAll");
    }

    @Override
    public List<Version> getVersions(ByteArray key) {
        StoreUtils.assertValidKey(key);
        GetVersionsClientRequest clientRequest = new GetVersionsClientRequest(getName(),
                                                                              requestFormat,
                                                                              requestRoutingType,
                                                                              key);
        if(logger.isDebugEnabled())
            logger.debug("GETVERSIONS keyRef: " + System.identityHashCode(key) + " requestRef: "
                         + System.identityHashCode(clientRequest));
        return request(clientRequest, "getVersions");
    }

    @Override
    public void put(ByteArray key, Versioned<byte[]> versioned, byte[] transforms)
            throws VoldemortException {
        StoreUtils.assertValidKey(key);
        PutClientRequest clientRequest = new PutClientRequest(getName(),
                                                              requestFormat,
                                                              requestRoutingType,
                                                              key,
                                                              versioned,
                                                              transforms);
        if(logger.isDebugEnabled())
            logger.debug("PUT keyRef: " + System.identityHashCode(key) + " requestRef: "
                         + System.identityHashCode(clientRequest));
        request(clientRequest, "put");
    }

    @Override
    public Object getCapability(StoreCapabilityType capability) {
        if(StoreCapabilityType.SOCKET_POOL.equals(capability))
            return this.pool;
        else
            throw new NoSuchCapabilityException(capability, getName());
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
        long startTimeMs = -1;
        long startTimeNs = -1;

        if(logger.isDebugEnabled()) {
            startTimeMs = System.currentTimeMillis();
        }
        startTimeNs = System.nanoTime();

        ClientRequestExecutor clientRequestExecutor = pool.checkout(destination);

        String debugMsgStr = "";

        BlockingClientRequest<T> blockingClientRequest = null;
        try {
            blockingClientRequest = new BlockingClientRequest<T>(delegate, timeoutMs);
            clientRequestExecutor.addClientRequest(blockingClientRequest,
                                                   timeoutMs,
                                                   System.nanoTime() - startTimeNs);
            blockingClientRequest.await();

            if(logger.isDebugEnabled())
                debugMsgStr += "success";

            return blockingClientRequest.getResult();
        } catch(InterruptedException e) {

            if(logger.isDebugEnabled())
                debugMsgStr += "unreachable: " + e.getMessage();

            throw new UnreachableStoreException("Failure in " + operationName + " on "
                                                + destination + ": " + e.getMessage(), e);
        } catch(IOException e) {
            clientRequestExecutor.close();

            if(logger.isDebugEnabled())
                debugMsgStr += "failure: " + e.getMessage();

            throw new UnreachableStoreException("Failure in " + operationName + " on "
                                                + destination + ": " + e.getMessage(), e);
        } finally {
            if(blockingClientRequest != null && !blockingClientRequest.isComplete()) {
                // close the executor if we timed out
                clientRequestExecutor.close();
            }

            if(logger.isDebugEnabled()) {
                logger.debug("Sync request end, type: "
                             + operationName
                             + " requestRef: "
                             + System.identityHashCode(delegate)
                             + " totalTimeNs: "
                             + (System.nanoTime() - startTimeNs)
                             + " start time: "
                             + startTimeMs
                             + " end time: "
                             + System.currentTimeMillis()
                             + " client:"
                             + clientRequestExecutor.getSocketChannel().socket().getLocalAddress()
                             + ":"
                             + clientRequestExecutor.getSocketChannel().socket().getLocalPort()
                             + " server: "
                             + clientRequestExecutor.getSocketChannel()
                                                    .socket()
                                                    .getRemoteSocketAddress() + " outcome: "
                             + debugMsgStr);
            }

            pool.checkin(destination, clientRequestExecutor);
        }
    }

    /**
     * This method handles submitting and then waiting for the request from the
     * server. It uses the ClientRequest API to actually write the request and
     * then read back the response. This implementation will not block for a
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
        pool.submitAsync(this.destination, delegate, callback, timeoutMs, operationName);
    }
}
