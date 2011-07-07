/*
 * Copyright 2011 LinkedIn, Inc
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.log4j.Logger;

import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.admin.AsyncOperation;
import voldemort.store.metadata.MetadataStore;

/**
 * Individual rebalancing operation run on the server side as an async
 * operation. This is run on the donor node
 */
public class DonorBasedRebalanceAsyncOperation extends AsyncOperation {

    private final static Logger logger = Logger.getLogger(DonorBasedRebalanceAsyncOperation.class);

    private List<Integer> rebalanceStatusList;
    private AdminClient adminClient;

    private final ExecutorService executors;
    private final List<RebalancePartitionsInfo> stealInfos;
    private final VoldemortConfig voldemortConfig;
    private final MetadataStore metadataStore;

    protected ExecutorService createExecutors(int numThreads) {

        return Executors.newFixedThreadPool(numThreads, new ThreadFactory() {

            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName(r.getClass().getName());
                return thread;
            }
        });
    }

    public DonorBasedRebalanceAsyncOperation(VoldemortConfig voldemortConfig,
                                             MetadataStore metadataStore,
                                             int requestId,
                                             List<RebalancePartitionsInfo> stealInfos) {
        super(requestId, "Donor based rebalance operation : " + stealInfos);
        this.voldemortConfig = voldemortConfig;
        this.metadataStore = metadataStore;
        this.stealInfos = stealInfos;
        this.rebalanceStatusList = new ArrayList<Integer>();
        this.adminClient = null;
        this.executors = createExecutors(voldemortConfig.getMaxParallelStoresRebalancing());
    }

    @Override
    public void operate() throws Exception {}

    @Override
    public void stop() {}

}
