/*
 * Copyright 2013 LinkedIn, Inc
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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.MessageEvent;

import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxManaged;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.store.CompositeVoldemortRequest;
import voldemort.store.stats.StoreStats;
import voldemort.utils.ByteArray;
import voldemort.utils.JmxUtils;

/**
 * A Wrapper class to provide asynchronous API for calling the fat client
 * methods. These methods will be invoked by the Netty request handler instead
 * of invoking the Fat Client methods on its own
 * 
 */
@JmxManaged(description = "A Wrapper for a Fat client in order to execute requests asynchronously")
public class FatClientWrapper {

    private ThreadPoolExecutor fatClientExecutor;
    private SocketStoreClientFactory storeClientFactory;
    private DynamicTimeoutStoreClient<ByteArray, byte[]> dynamicTimeoutClient;
    private final CoordinatorConfig coordinatorConfig;
    private final Logger logger = Logger.getLogger(FatClientWrapper.class);
    private final String storeName;
    private final CoordinatorErrorStats errorStats;
    private final StoreStats coordinatorPerfStats;

    /**
     * 
     * @param storeName Store to connect to via this fat client
     * @param config Bootstrap URLs for the intended cluster
     * @param clientConfig The config used to bootstrap the fat client
     * @param storesXml Stores XML used to bootstrap the fat client
     * @param clusterXml Cluster XML used to bootstrap the fat client
     * @param errorStats
     * @param coordinatorPerfStats
     */
    public FatClientWrapper(String storeName,
                            CoordinatorConfig config,
                            ClientConfig clientConfig,
                            String storesXml,
                            String clusterXml,
                            CoordinatorErrorStats errorStats,
                            StoreStats coordinatorPerfStats) {

        this.coordinatorConfig = config;

        // TODO: Import this from Config
        this.fatClientExecutor = new ThreadPoolExecutor(clientConfig.getFatClientWrapperCorePoolSize(),
                                                        clientConfig.getFatClientWrapperMaxPoolSize(),
                                                        clientConfig.getFatClientWrapperKeepAliveInSecs(), // Keepalive
                                                        TimeUnit.SECONDS, // Keepalive
                                                                          // Timeunit
                                                        new ArrayBlockingQueue<Runnable>(clientConfig.getFatClientWrapperMaxPoolSize(),
                                                                                         true),

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

        this.storeClientFactory = new SocketStoreClientFactory(clientConfig);
        this.dynamicTimeoutClient = new DynamicTimeoutStoreClient<ByteArray, byte[]>(storeName,
                                                                                     this.storeClientFactory,
                                                                                     1,
                                                                                     storesXml,
                                                                                     clusterXml);
        this.errorStats = errorStats;
        this.coordinatorPerfStats = coordinatorPerfStats;
        this.storeName = storeName;

        // Register the Mbean
        JmxUtils.registerMbean(this,
                               JmxUtils.createObjectName(JmxUtils.getPackageName(this.getClass()),
                                                         JmxUtils.getClassName(this.getClass())
                                                                 + "-" + storeName));

    }

    public void close() {
        // Register the Mbean
        JmxUtils.unregisterMbean(JmxUtils.createObjectName(JmxUtils.getPackageName(this.getClass()),
                                                           JmxUtils.getClassName(this.getClass())
                                                                   + "-" + this.storeName));
        this.storeClientFactory.close();
    }

    /**
     * Perform a get operation on the fat client asynchronously
     * 
     * @param getRequestObject Contains the key used in the get operation
     * @param getRequestMessageEvent MessageEvent to write the response back to
     * @param startTimestampInNs The start timestamp used to measure turnaround
     *        time
     */
    void submitGetRequest(final CompositeVoldemortRequest<ByteArray, byte[]> getRequestObject,
                          final MessageEvent getRequestMessageEvent,
                          long startTimestampInNs) {
        try {

            this.fatClientExecutor.submit(new HttpGetRequestExecutor(getRequestObject,
                                                                     getRequestMessageEvent,
                                                                     this.dynamicTimeoutClient,
                                                                     startTimestampInNs,
                                                                     this.coordinatorPerfStats));
            if(logger.isDebugEnabled()) {
                logger.debug("Submitted a get request");
            }

        } catch(RejectedExecutionException rej) {
            handleRejectedException(rej, getRequestMessageEvent);
        }
    }

    /**
     * Perform a getAll operation on the fat client asynchronously
     * 
     * @param getAllRequestObject Contains the keys used in the getAll oepration
     * @param getAllRequestMessageEvent MessageEvent to write the response back
     *        to
     * @param storeName Name of the store to be specified in the response
     *        (header)
     * @param startTimestampInNs The start timestamp used to measure turnaround
     *        time
     */
    void submitGetAllRequest(final CompositeVoldemortRequest<ByteArray, byte[]> getAllRequestObject,
                             final MessageEvent getAllRequestMessageEvent,
                             final String storeName,
                             long startTimestampInNs) {
        try {

            this.fatClientExecutor.submit(new HttpGetAllRequestExecutor(getAllRequestObject,
                                                                        getAllRequestMessageEvent,
                                                                        this.dynamicTimeoutClient,
                                                                        storeName,
                                                                        startTimestampInNs,
                                                                        this.coordinatorPerfStats));
            if(logger.isDebugEnabled()) {
                logger.debug("Submitted a get all request");
            }

        } catch(RejectedExecutionException rej) {
            handleRejectedException(rej, getAllRequestMessageEvent);
        }
    }

    /**
     * Interface to perform put operation on the Fat client
     * 
     * @param putRequestObject Request object containing the key and value
     * @param putRequestMessageEvent MessageEvent to write the response on.
     * @param startTimestampInNs The start timestamp used to measure turnaround
     *        time
     */
    void submitPutRequest(final CompositeVoldemortRequest<ByteArray, byte[]> putRequestObject,
                          final MessageEvent putRequestMessageEvent,
                          long startTimestampInNs) {
        try {

            this.fatClientExecutor.submit(new HttpPutRequestExecutor(putRequestObject,
                                                                     putRequestMessageEvent,
                                                                     this.dynamicTimeoutClient,
                                                                     startTimestampInNs,
                                                                     this.coordinatorPerfStats));
            if(logger.isDebugEnabled()) {
                logger.debug("Submitted a put request");
            }

        } catch(RejectedExecutionException rej) {
            handleRejectedException(rej, putRequestMessageEvent);
        }
    }

    /**
     * Interface to perform delete operation on the fat client
     * 
     * @param deleteRequestObject Contains the key and the version used in the
     *        delete operation
     * @param deleteRequestEvent MessageEvent to write the response back to
     * @param startTimestampInNs The start timestamp used to measure turnaround
     *        time
     */
    public void submitDeleteRequest(CompositeVoldemortRequest<ByteArray, byte[]> deleteRequestObject,
                                    MessageEvent deleteRequestEvent,
                                    long startTimestampInNs) {
        try {

            this.fatClientExecutor.submit(new HttpDeleteRequestExecutor(deleteRequestObject,
                                                                        deleteRequestEvent,
                                                                        this.dynamicTimeoutClient,
                                                                        startTimestampInNs,
                                                                        this.coordinatorPerfStats));

        } catch(RejectedExecutionException rej) {
            handleRejectedException(rej, deleteRequestEvent);
        }

    }

    /**
     * Perform a get schemata operation without going over the wire
     * 
     */
    void submitGetSchemataRequest(final MessageEvent getRequestMessageEvent) {
        try {

            this.fatClientExecutor.submit(new GetSchemataRequestExecutor(getRequestMessageEvent,
                                                                         storeName,
                                                                         storeClientFactory));
            if(logger.isDebugEnabled()) {
                logger.debug("Submitted a get schemata request");
            }

        } catch(RejectedExecutionException rej) {
            handleRejectedException(rej, getRequestMessageEvent);
        }
    }

    // TODO: Add a custom HTTP Error status 429: Too many requests
    private void handleRejectedException(RejectedExecutionException rej, MessageEvent getRequest) {
        this.errorStats.reportException(rej);
        logger.error("rejected !!!");
        getRequest.getChannel().write(null); // Write error back to the thin
                                             // client
    }

    @JmxGetter(name = "numberOfActiveThreads", description = "The number of active Fat client wrapper threads.")
    public int getNumberOfActiveThreads() {
        return this.fatClientExecutor.getActiveCount();
    }

    @JmxGetter(name = "numberOfThreads", description = "The total number of Fat client wrapper threads, active and idle.")
    public int getNumberOfThreads() {
        return this.fatClientExecutor.getPoolSize();
    }

    @JmxGetter(name = "queuedRequests", description = "Number of requests in the Fat client wrapper queue waiting to execute.")
    public int getQueuedRequests() {
        return this.fatClientExecutor.getQueue().size();
    }
}
