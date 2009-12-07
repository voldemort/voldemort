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
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;

/**
 * FailureDetectorConfig abstracts away the complexities of configuring the
 * FailureDetector implementation between server, client, and testing
 * environments.
 * 
 * <p/>
 * 
 * Not all data provided by the FailureDetectorConfig need be used by all
 * FailureDetector implementations.
 * 
 * @author Kirk True
 */

public interface FailureDetectorConfig {

    /**
     * Returns the fully-qualified class name of the FailureDetector
     * implementation.
     * 
     * @return Class name to instantiate for the FailureDetector
     */

    public String getImplementationClassName();

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

    public long getNodeBannagePeriod();

    /**
     * Returns a list of nodes in the cluster represented by this failure
     * detector configuration.
     * 
     * @return Collection of Node instances, usually determined from the Cluster
     *         object (except in the case of unit tests, perhaps)
     */

    public Collection<Node> getNodes();

    /**
     * Returns a Store for this node. This is used by some FailureDetector
     * implementations to attempt contact with the node before marking said node
     * as available.
     * 
     * @param node Node to access
     * 
     * @return Store to access to determine node availability
     */

    public Store<ByteArray, byte[]> getStore(Node node);

    public Time getTime();

}
