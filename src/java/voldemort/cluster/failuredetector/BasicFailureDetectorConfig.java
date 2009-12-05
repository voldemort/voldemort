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
import java.util.Map;

import voldemort.cluster.Node;
import voldemort.store.Store;
import voldemort.utils.ByteArray;

/**
 * BasicFailureDetectorConfig simply holds all the data that was available to it
 * upon construction. It is often used for testing rather than for server or
 * client configuration.
 * 
 * @author Kirk True
 */

public class BasicFailureDetectorConfig implements FailureDetectorConfig {

    private final String implementationClassName;

    private final long nodeBannagePeriod;

    private final Collection<Node> nodes;

    private final Map<Integer, Store<ByteArray, byte[]>> stores;

    public BasicFailureDetectorConfig(String implementationClassName,
                                      long nodeBannagePeriod,
                                      Collection<Node> nodes,
                                      Map<Integer, Store<ByteArray, byte[]>> stores) {
        this.implementationClassName = implementationClassName;
        this.nodeBannagePeriod = nodeBannagePeriod;
        this.nodes = nodes;
        this.stores = stores;
    }

    public String getImplementationClassName() {
        return implementationClassName;
    }

    public long getNodeBannagePeriod() {
        return nodeBannagePeriod;
    }

    public Collection<Node> getNodes() {
        return nodes;
    }

    public Store<ByteArray, byte[]> getStore(Node node) {
        return stores.get(node.getId());
    }

}
