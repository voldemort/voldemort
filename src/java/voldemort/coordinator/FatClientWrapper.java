/*
 * Copyright 2008-2013 LinkedIn, Inc
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

package voldemort.coordinator;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.MessageEvent;

import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.store.CompositeVoldemortRequest;
import voldemort.utils.ByteArray;

/**
 * A Wrapper class to provide asynchronous API for calling the fat client
 * methods. These methods will be invoked by the Netty request handler instead
 * of invoking the Fat Client methods on its own
 * 
 */
public class FatClientWrapper {

    private ExecutorService fatClientExecutor;
    private SocketStoreClientFactory storeClientFactory;
    private DynamicTimeoutStoreClient<ByteArray, byte[]> dynamicTimeoutClient;
    private final CoordinatorConfig config;
    private final Logger logger = Logger.getLogger(FatClientWrapper.class);

    /**
     * 
     * @param storeName Store to connect to via this fat client
     * @param config Bootstrap URLs for the intended cluster
     * @param clientConfig The config used to bootstrap the fat client
     * @param storesXml Stores XML used to bootstrap the fat client
     * @param clusterXml Cluster XML used to bootstrap the fat client
     */
    public FatClientWrapper(String storeName,
                            CoordinatorConfig config,
                            ClientConfig clientConfig,
                            String storesXml,
                            String clusterXml) {

        this.config = config;

        // TODO: Import this from Config
        this.fatClientExecutor = new ThreadPoolExecutor(this.config.getFatClientWrapperCorePoolSize(),
                                                        this.config.getFatClientWrapperMaxPoolSize(),
                                                        this.config.getFatClientWrapperKeepAliveInSecs(), // Keepalive
                                                        TimeUnit.SECONDS, // Keepalive
                                                                          // Timeunit
                                                        new SynchronousQueue<Runnable>(), // Queue
                                                                                          // for
                                                                                          // pending
                                                                                          // tasks

                                                        new ThreadFactory() {

                                                            @Override
                                                            public Thread newThread(Runnable r) {
                                                                Thread t = new Thread(r);
                                                                t.setName("FatClientExecutor");
                                                                return t;
                                                            }
                                                        },

                                                        new RejectedExecutionHandler() { // Handler

                                                            // for
                                                            // rejected
                                                            // tasks

                                                            @Override
                                                            public void rejectedExecution(Runnable r,
                                                                                          ThreadPoolExecutor executor) {

                                                            }
                                                        });
        // this.fatClientRequestQueue = new SynchronousQueue<Future>();

        this.storeClientFactory = new SocketStoreClientFactory(clientConfig);
        this.dynamicTimeoutClient = new DynamicTimeoutStoreClient<ByteArray, byte[]>(storeName,
                                                                                     this.storeClientFactory,
                                                                                     1,
                                                                                     storesXml,
                                                                                     clusterXml);

    }

    /**
     * Perform a get operation on the fat client asynchronously
     * 
     * @param getRequestObject Contains the key used in the get operation
     * @param getRequestMessageEvent MessageEvent to write the response back to
     */
    void submitGetRequest(final CompositeVoldemortRequest<ByteArray, byte[]> getRequestObject,
                          final MessageEvent getRequestMessageEvent) {
        try {

            this.fatClientExecutor.submit(new HttpGetRequestExecutor(getRequestObject,
                                                                     getRequestMessageEvent,
                                                                     this.dynamicTimeoutClient));
            if(logger.isDebugEnabled()) {
                logger.debug("Submitted a get request");
            }

            // Keep track of this request for monitoring
            // this.fatClientRequestQueue.add(f);
        } catch(RejectedExecutionException rej) {
            handleRejectedException(getRequestMessageEvent);
        }
    }

    /**
     * Perform a getAll operation on the fat client asynchronously
     * 
     * @param getAllRequestObject Contains the keys used in the getAll oepration
     * @param getAllRequestMessageEvent MessageEvent to write the response back
     *        to
     */
    void submitGetAllRequest(final CompositeVoldemortRequest<ByteArray, byte[]> getAllRequestObject,
                             final MessageEvent getAllRequestMessageEvent,
                             final String storeName) {
        try {

            this.fatClientExecutor.submit(new HttpGetAllRequestExecutor(getAllRequestObject,
                                                                        getAllRequestMessageEvent,
                                                                        this.dynamicTimeoutClient,
                                                                        storeName));
            if(logger.isDebugEnabled()) {
                logger.debug("Submitted a get all request");
            }

            // Keep track of this request for monitoring
            // this.fatClientRequestQueue.add(f);
        } catch(RejectedExecutionException rej) {
            handleRejectedException(getAllRequestMessageEvent);
        }
    }

    /**
     * Interface to perform put operation on the Fat client
     * 
     * @param key: ByteArray representation of the key to put
     * @param value: value corresponding to the key to put
     * @param putRequest: MessageEvent to write the response on.
     * @param operationTimeoutInMs The timeout value for this operation
     */
    void submitPutRequest(final CompositeVoldemortRequest<ByteArray, byte[]> putRequestObject,
                          final MessageEvent putRequest) {
        try {

            this.fatClientExecutor.submit(new HttpPutRequestExecutor(putRequestObject,
                                                                     putRequest,
                                                                     this.dynamicTimeoutClient));
            if(logger.isDebugEnabled()) {
                logger.debug("Submitted a put request");
            }

            // Keep track of this request for monitoring
            // this.fatClientRequestQueue.add(f);
        } catch(RejectedExecutionException rej) {
            handleRejectedException(putRequest);
        }
    }

    /**
     * Interface to perform delete operation on the fat client
     * 
     * @param deleteRequestObject Contains the key and the version used in the
     *        delete operation
     * @param deleteRequestEvent MessageEvent to write the response back to
     */
    public void submitDeleteRequest(CompositeVoldemortRequest<ByteArray, byte[]> deleteRequestObject,
                                    MessageEvent deleteRequestEvent) {
        try {

            this.fatClientExecutor.submit(new HttpDeleteRequestExecutor(deleteRequestObject,
                                                                        deleteRequestEvent,
                                                                        this.dynamicTimeoutClient));

            // Keep track of this request for monitoring
            // this.fatClientRequestQueue.add(f);
        } catch(RejectedExecutionException rej) {
            handleRejectedException(deleteRequestEvent);
        }

    }

    // TODO: Add a custom HTTP Error status 429: Too many requests
    private void handleRejectedException(MessageEvent getRequest) {
        logger.error("rejected !!!");
        getRequest.getChannel().write(null); // Write error back to the thin
                                             // client
                                             // String errorDescription =
                                             // "Request queue for store " +
                                             // this.dynamicTimeoutClient.getStoreName()
                                             // + " is full !");
        // logger.error(errorDescription);
        // RESTErrorHandler.handleError(REQUEST_TIMEOUT,
        // this.getRequestMessageEvent,
        // false,
        // errorDescription);
    }

}
