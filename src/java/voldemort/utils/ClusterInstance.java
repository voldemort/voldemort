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

package voldemort.utils;

import java.util.List;

import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;


/**
 * This class wraps up a Cluster object and a List<StoreDefinition>. The methods
 * are effectively helper or util style methods for analyzing partitions and so
 * on which are a function of both Cluster and List<StoreDefinition>.
 */
public class ClusterInstance {

    // TODO: (refactor) Improve upon the name "ClusterInstance". Object-oriented
    // meaning of 'instance' is too easily confused with system notion of an
    // "instance of a cluster" (the intended usage in this class name).

    private final Cluster cluster;
    private final List<StoreDefinition> storeDefs;

    public ClusterInstance(Cluster cluster, List<StoreDefinition> storeDefs) {
        this.cluster = cluster;
        this.storeDefs = storeDefs;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public List<StoreDefinition> getStoreDefs() {
        return storeDefs;
    }

    /**
     * Outputs an analysis of how balanced the cluster is given the store
     * definitions. The metric max/min ratio is used to describe balance. The
     * max/min ratio is the ratio of largest number of store-partitions to
     * smallest number of store-partitions). If the minimum number of
     * store-partitions is zero, then the max/min ratio is set to max rather
     * than to infinite.
     * 
     * @return First element of pair is the max/min ratio. Second element of
     *         pair is a string that can be printed to dump all the gory details
     *         of the analysis.
     */
    public PartitionBalance getPartitionBalance() {

        return new PartitionBalance(cluster, storeDefs);
    }
}
