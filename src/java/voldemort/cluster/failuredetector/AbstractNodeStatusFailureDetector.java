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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
        nodeStatusMap = new ConcurrentHashMap<Node, NodeStatus>();

        for(Node node: failureDetectorConfig.getNodes())
            nodeStatusMap.put(node, new NodeStatus(failureDetectorConfig.getTime()));
    }

    public long getLastChecked(Node node) {
        return getNodeStatus(node).getLastCheckedMs();
    }

    public boolean isAvailable(Node node) {
        return isAvailable(getNodeStatus(node));
    }

    protected void setAvailable(Node node) {
        getNodeStatus(node).setAvailable();
        notifyAvailable(node);
    }

    protected void setUnavailable(Node node) {
        getNodeStatus(node).setUnavailable();
        notifyUnavailable(node);
    }

    protected boolean isAvailable(NodeStatus nodeStatus) {
        return !nodeStatus.isUnavailable(failureDetectorConfig.getNodeBannagePeriod());
    }

    private NodeStatus getNodeStatus(Node node) {
        NodeStatus nodeStatus = nodeStatusMap.get(node);

        if(nodeStatus == null)
            throw new IllegalArgumentException(node.getId()
                                               + " is not a valid node for this cluster");
        return nodeStatus;
    }

}
