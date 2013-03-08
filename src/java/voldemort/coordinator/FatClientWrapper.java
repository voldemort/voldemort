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
import voldemort.store.VoldemortRequestWrapper;

/**
 * A Wrapper class to provide asynchronous API for calling the fat client
 * methods. These methods will be invoked by the Netty request handler instead
 * of invoking the Fat Client methods on its own
 * 
 */
public class FatClientWrapper {

    private ExecutorService fatClientExecutor;
    private SocketStoreClientFactory storeClientFactory;
    private final Logger logger = Logger.getLogger(FatClientWrapper.class);
    private DynamicTimeoutStoreClient dynamicTimeoutClient;

    /**
     * 
     * @param storeName Store to connect to via this fat client
     * @param bootstrapURLs Bootstrap URLs for the intended cluster
     * @param clientConfig The config used to bootstrap the fat client
     * @param storesXml Stores XML used to bootstrap the fat client
     * @param clusterXml Cluster XML used to bootstrap the fat client
     */
    public FatClientWrapper(String storeName,
                            String[] bootstrapURLs,
                            ClientConfig clientConfig,
                            String storesXml,
                            String clusterXml) {

        // TODO: Import this from Config
        this.fatClientExecutor = new ThreadPoolExecutor(20, // Core pool size
                                                        20, // Max pool size
                                                        60, // Keepalive
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
        this.dynamicTimeoutClient = new DynamicTimeoutStoreClient<Object, Object>(storeName,
                                                                                  this.storeClientFactory,
                                                                                  1,
                                                                                  storesXml,
                                                                                  clusterXml);

    }

    /**
     * Interface to do get from the Fat client
     * 
     * 
     * @param key: ByteArray representation of the key to get received from the
     *        thin client
     * @param getRequest: MessageEvent to write the response on.
     * @param operationTimeoutInMs The timeout value for this operation
     * @param resolveConflicts Determines whether the default resolver should be
     *        used in case of conflicts
     */
    void submitGetRequest(final VoldemortRequestWrapper getRequestObject,
                          final MessageEvent getRequest) {
        try {

            this.fatClientExecutor.submit(new GetRequestExecutor(getRequestObject,
                                                                 getRequest,
                                                                 this.dynamicTimeoutClient));

            // Keep track of this request for monitoring
            // this.fatClientRequestQueue.add(f);
        } catch(RejectedExecutionException rej) {
            handleRejectedException(getRequest);
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
    void submitPutRequest(final VoldemortRequestWrapper putRequestObject,
                          final MessageEvent putRequest) {
        try {

            this.fatClientExecutor.submit(new PutRequestExecutor(putRequestObject,
                                                                 putRequest,
                                                                 this.dynamicTimeoutClient));

            // Keep track of this request for monitoring
            // this.fatClientRequestQueue.add(f);
        } catch(RejectedExecutionException rej) {
            handleRejectedException(putRequest);
        }
    }

    private void handleRejectedException(MessageEvent getRequest) {
        getRequest.getChannel().write(null); // Write error back to the thin
                                             // client
    }

}
