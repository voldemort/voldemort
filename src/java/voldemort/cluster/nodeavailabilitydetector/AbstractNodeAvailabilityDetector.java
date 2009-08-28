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

package voldemort.cluster.nodeavailabilitydetector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.MBeanOperationInfo;

import org.apache.log4j.Logger;

import voldemort.annotations.jmx.JmxManaged;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.cluster.Node;
import voldemort.store.Store;
import voldemort.utils.ByteArray;

@JmxManaged(description = "Detects the availability of the physical servers on which a Voldemort cluster runs")
public abstract class AbstractNodeAvailabilityDetector implements NodeAvailabilityDetector {

    protected Map<Integer, Store<ByteArray, byte[]>> stores;

    protected long nodeBannageMs;

    protected final Map<Node, NodeStatus> nodeStatusMap;

    protected final Logger logger = Logger.getLogger(getClass().getName());

    protected AbstractNodeAvailabilityDetector() {
        nodeStatusMap = new HashMap<Node, NodeStatus>();
    }

    public long getLastChecked(Node node) {
        return getNodeStatus(node).getLastCheckedMs();
    }

    public Map<Integer, Store<ByteArray, byte[]>> getStores() {
        return stores;
    }

    public void setStores(Map<Integer, Store<ByteArray, byte[]>> stores) {
        this.stores = stores;
    }

    public long getNodeBannageMs() {
        return nodeBannageMs;
    }

    public void setNodeBannageMs(long nodeBannageMs) {
        this.nodeBannageMs = nodeBannageMs;
    }

    @JmxOperation(impact = MBeanOperationInfo.INFO, description = "The number of available nodes")
    public int getNumberOfAvailableNodes() {
        List<NodeStatus> nodeStatuses = null;

        synchronized(nodeStatusMap) {
            nodeStatuses = new ArrayList<NodeStatus>(nodeStatusMap.values());
        }

        int available = 0;

        for(NodeStatus nodeStatus: nodeStatuses)
            if(nodeStatus.isAvailable())
                available++;

        return available;
    }

    protected NodeStatus getNodeStatus(Node node) {
        synchronized(nodeStatusMap) {
            NodeStatus nodeStatus = nodeStatusMap.get(node);

            if(nodeStatus == null) {
                nodeStatus = new NodeStatus();
                nodeStatusMap.put(node, nodeStatus);
            }

            return nodeStatus;
        }
    }

}
