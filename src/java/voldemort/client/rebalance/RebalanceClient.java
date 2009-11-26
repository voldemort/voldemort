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

package voldemort.client.rebalance;

import voldemort.annotations.concurrency.Threadsafe;
import voldemort.cluster.Cluster;
import voldemort.store.rebalancing.RedirectingStore;

/**
 * The cluster rebalancing Interface.
 * 
 */
@Threadsafe
public interface RebalanceClient {

    /**
     * Voldemort online rebalancing mechanism. <br>
     * Compares the provided currentCluster and targetCluster and makes a list
     * of partitions need to be transferred <br>
     * The cluster is kept consistent during rebalancing using a proxy mechanism
     * via {@link RedirectingStore}
     * 
     * @param storeName : store to be rebalanced
     * @param currentCluster: currentCluster configuration.
     * @param targetCluster: target Cluster configuration
     */
    public void rebalance(final String storeName,
                          final Cluster currentCluster,
                          final Cluster targetCluster);

}
