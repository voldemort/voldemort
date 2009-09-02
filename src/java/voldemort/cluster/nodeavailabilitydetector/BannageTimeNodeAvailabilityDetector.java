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

import org.apache.log4j.Level;

import voldemort.cluster.Node;

public class BannageTimeNodeAvailabilityDetector extends AbstractNodeAvailabilityDetector {

    public BannageTimeNodeAvailabilityDetector(NodeAvailabilityDetectorConfig nodeAvailabilityDetectorConfig) {
        super(nodeAvailabilityDetectorConfig);
    }

    public boolean isAvailable(Node node) {
        return !getNodeStatus(node).isUnavailable(nodeAvailabilityDetectorConfig.getNodeBannagePeriod());
    }

    public void recordException(Node node, Exception e) {
        getNodeStatus(node).setUnavailable();

        if(logger.isEnabledFor(Level.WARN))
            logger.warn("Could not connect to node " + node.getId() + " at " + node.getHost()
                        + " marking as unavailable for "
                        + nodeAvailabilityDetectorConfig.getNodeBannagePeriod() + " ms.", e);

        if(logger.isDebugEnabled())
            logger.debug(e);
    }

    public void recordSuccess(Node node) {
        getNodeStatus(node).setAvailable();
    }

}
