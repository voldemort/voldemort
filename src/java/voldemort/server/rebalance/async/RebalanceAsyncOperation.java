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

package voldemort.server.rebalance.async;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import voldemort.client.protocol.admin.AdminClient;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.admin.AsyncOperation;
import voldemort.server.rebalance.Rebalancer;
import voldemort.store.metadata.MetadataStore;

public abstract class RebalanceAsyncOperation extends AsyncOperation {

    protected final static Logger logger = Logger.getLogger(RebalanceAsyncOperation.class);

    protected final VoldemortConfig voldemortConfig;
    protected final MetadataStore metadataStore;
    protected AdminClient adminClient;
    protected final ExecutorService executors;

    protected Rebalancer rebalancer;

    protected ExecutorService createExecutors(int numThreads) {

        return Executors.newFixedThreadPool(numThreads, new ThreadFactory() {

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName(r.getClass().getName());
                return thread;
            }
        });
    }

    public RebalanceAsyncOperation(Rebalancer rebalancer,
                                   VoldemortConfig voldemortConfig,
                                   MetadataStore metadataStore,
                                   int requestId,
                                   String operationString) {
        super(requestId, operationString);
        this.voldemortConfig = voldemortConfig;
        this.metadataStore = metadataStore;
        this.adminClient = null;
        this.executors = createExecutors(voldemortConfig.getMaxParallelStoresRebalancing());
        this.rebalancer = rebalancer;
    }

    protected void waitForShutdown() {
        try {
            executors.shutdown();
            executors.awaitTermination(voldemortConfig.getRebalancingTimeoutSec(), TimeUnit.SECONDS);
        } catch(InterruptedException e) {
            logger.error("Interrupted while awaiting termination for executors.", e);
        }
    }
}
