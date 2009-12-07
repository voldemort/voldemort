/*
 * Copyright 2009 Mustard Grain, Inc.
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

package voldemort.cluster.failuredetector;

import java.util.Collection;

import voldemort.client.ClientConfig;
import voldemort.cluster.Node;
import voldemort.server.VoldemortConfig;
import voldemort.utils.SystemTime;
import voldemort.utils.Time;

/**
 * FailureDetectorConfig simply holds all the data that was available to it upon
 * construction.
 * 
 * @author Kirk True
 */

public class FailureDetectorConfig {

    protected String implementationClassName;

    protected long nodeBannagePeriod = 30000;

    protected Collection<Node> nodes;

    protected StoreResolver storeResolver;

    protected boolean isJmxEnabled = false;

    protected Time time = SystemTime.INSTANCE;

    /**
     * Returns the fully-qualified class name of the FailureDetector
     * implementation.
     * 
     * @return Class name to instantiate for the FailureDetector
     */

    public String getImplementationClassName() {
        return implementationClassName;
    }

    public FailureDetectorConfig setImplementationClassName(String implementationClassName) {
        this.implementationClassName = implementationClassName;
        return this;
    }

    /**
     * Returns the node bannage period (in milliseconds) as defined by the
     * client or server configuration. Some FailureDetector implementations wait
     * for a specified period of time before attempting to access the node again
     * once it has become unavailable.
     * 
     * @return Period of bannage of a node, in milliseconds
     * 
     * @see VoldemortConfig#getClientNodeBannageMs
     * @see ClientConfig#getNodeBannagePeriod
     */

    public long getNodeBannagePeriod() {
        return nodeBannagePeriod;
    }

    public FailureDetectorConfig setNodeBannagePeriod(long nodeBannagePeriod) {
        this.nodeBannagePeriod = nodeBannagePeriod;
        return this;
    }

    /**
     * Returns a list of nodes in the cluster represented by this failure
     * detector configuration.
     * 
     * @return Collection of Node instances, usually determined from the Cluster
     *         object (except in the case of unit tests, perhaps)
     */

    public Collection<Node> getNodes() {
        return nodes;
    }

    public FailureDetectorConfig setNodes(Collection<Node> nodes) {
        this.nodes = nodes;
        return this;
    }

    public StoreResolver getStoreResolver() {
        return storeResolver;
    }

    public FailureDetectorConfig setStoreResolver(StoreResolver storeResolver) {
        this.storeResolver = storeResolver;
        return this;
    }

    public boolean isJmxEnabled() {
        return isJmxEnabled;
    }

    public FailureDetectorConfig setJmxEnabled(boolean isJmxEnabled) {
        this.isJmxEnabled = isJmxEnabled;
        return this;
    }

    public Time getTime() {
        return time;
    }

    public FailureDetectorConfig setTime(Time time) {
        this.time = time;
        return this;
    }

}
