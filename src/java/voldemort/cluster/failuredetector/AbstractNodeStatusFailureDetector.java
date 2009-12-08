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

import java.util.HashMap;
import java.util.Map;

import voldemort.cluster.Node;

/**
 * AbstractNodeStatusFailureDetector serves as a building block for
 * FailureDetector implementations.
 * 
 * @author Kirk True
 */

public abstract class AbstractNodeStatusFailureDetector extends AbstractFailureDetector {

    protected final Map<Node, NodeStatus> nodeStatusMap;

    protected AbstractNodeStatusFailureDetector(FailureDetectorConfig failureDetectorConfig) {
        super(failureDetectorConfig);
        nodeStatusMap = new HashMap<Node, NodeStatus>();

        for(Node node: failureDetectorConfig.getNodes())
            nodeStatusMap.put(node, new NodeStatus(failureDetectorConfig.getTime()));
    }

    public long getLastChecked(Node node) {
        NodeStatus nodeStatus = getNodeStatus(node);
        return nodeStatus.getLastCheckedMs();
    }

    public boolean isAvailable(Node node) {
        NodeStatus nodeStatus = getNodeStatus(node);

        // The node can be available in one of two ways: a) it's actually
        // available, or b) it was unavailable but our "bannage" period has
        // expired so we're free to consider it as available.
        boolean isAvailable = !nodeStatus.isUnavailable(failureDetectorConfig.getNodeBannagePeriod());

        // If we're now considered available but our actual status is *not*
        // available, this means we've become available via a timeout. We act
        // like we're fully available in that we send out the availability
        // event.
        if(isAvailable && !nodeStatus.isAvailable())
            notifyAvailable(node);

        return isAvailable;
    }

    public void setAvailable(Node node) {
        NodeStatus nodeStatus = getNodeStatus(node);

        // We need to distinguish the case where we're newly available and the
        // case where we're getting redundant availability notices. So let's
        // check the node status before we update it.
        boolean previouslyAvailable = nodeStatus.isAvailable();

        // Update our state to be available.
        nodeStatus.setAvailable();

        // If we were not previously available, we've just switched state, so
        // notify any listeners.
        if(!previouslyAvailable)
            notifyAvailable(node);
    }

    public void setUnavailable(Node node) {
        NodeStatus nodeStatus = getNodeStatus(node);

        // The node can be available in one of two ways: a) it's actually
        // available, or b) it was unavailable but our "bannage" period has
        // expired so we're free to consider it as available.
        boolean previouslyAvailable = !nodeStatus.isUnavailable(failureDetectorConfig.getNodeBannagePeriod());

        // Update our state to be unavailable.
        nodeStatus.setUnavailable();

        // If we were previously available, we've just switched state from
        // available to unavailable, so notify any listeners.
        if(previouslyAvailable)
            notifyUnavailable(node);
    }

    protected NodeStatus getNodeStatus(Node node) {
        NodeStatus nodeStatus = nodeStatusMap.get(node);

        if(nodeStatus == null)
            throw new IllegalArgumentException(node.getId()
                                               + " is not a valid node for this cluster");
        return nodeStatus;
    }

}
