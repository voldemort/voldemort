/*
 * Copyright 2009 Mustard Grain, Inc., 2009-2010 LinkedIn, Inc.
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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxManaged;
import voldemort.client.ClientConfig;
import voldemort.cluster.Node;
import voldemort.server.VoldemortConfig;
import voldemort.store.UnreachableStoreException;

/**
 * BannagePeriodFailureDetector relies on external callers to notify it of
 * failed attempts to access a node's store via recordException. When
 * recordException is invoked, the node is marked offline for a period of time
 * as defined by the client or server configuration. Once that period has
 * passed, the node is considered <em>available</em>. However,
 * BannagePeriodFailureDetector's definition of available uses a fairly loose
 * sense of the word. Rather than considering the node to be available for
 * access, it is available for <i>attempting</i> to access. In actuality the
 * node may still be down. However, the intent is simply to mark it down for N
 * seconds and then attempt to try again and repeat. If the node is truly
 * available for access, the caller will then invoke recordSuccess and the node
 * will be marked available in the truest sense of the word.
 * 
 * 
 * @see VoldemortConfig#getFailureDetectorBannagePeriod
 * @see ClientConfig#getFailureDetectorBannagePeriod
 * @see FailureDetectorConfig#getBannagePeriod
 */

@JmxManaged(description = "Detects the availability of the nodes on which a Voldemort cluster runs")
@Deprecated
public class BannagePeriodFailureDetector extends AbstractFailureDetector {

    public BannagePeriodFailureDetector(FailureDetectorConfig failureDetectorConfig) {
        super(failureDetectorConfig);
    }

    public boolean isAvailable(Node node) {
        checkNodeArg(node);

        long bannagePeriod = failureDetectorConfig.getBannagePeriod();
        long currentTime = failureDetectorConfig.getTime().getMilliseconds();

        NodeStatus nodeStatus = getNodeStatus(node);

        synchronized(nodeStatus) {
            // The node can be available in one of two ways: a) it's actually
            // available, or...
            if(nodeStatus.isAvailable())
                return true;

            // ...b) it was unavailable but our "bannage" period has expired so
            // we're free to consider it as available. If we're now considered
            // available but our actual status is *not* available, this means
            // we've become available via a timeout. We act like we're fully
            // available in that we send out the availability event.
            if(nodeStatus.getLastChecked() + bannagePeriod < currentTime) {
                setAvailable(node);
                return true;
            } else {
                return false;
            }
        }
    }

    public void recordException(Node node, long requestTime, UnreachableStoreException e) {
        checkArgs(node, requestTime);
        setUnavailable(node, e);
    }

    public void recordSuccess(Node node, long requestTime) {
        checkArgs(node, requestTime);
        setAvailable(node);
    }

    @JmxGetter(name = "unavailableNodesBannageExpiration", description = "List of unavailable nodes and their respective bannage expiration")
    public String getUnavailableNodesBannageExpiration() {
        List<String> list = new ArrayList<String>();
        long bannagePeriod = failureDetectorConfig.getBannagePeriod();
        long currentTime = failureDetectorConfig.getTime().getMilliseconds();

        for(Node node: getConfig().getCluster().getNodes()) {
            if(!isAvailable(node)) {
                NodeStatus nodeStatus = getNodeStatus(node);
                long millis = 0;

                synchronized(nodeStatus) {
                    millis = (nodeStatus.getLastChecked() + bannagePeriod) - currentTime;
                }

                list.add(node.getId() + "=" + millis);
            }
        }

        return StringUtils.join(list, ",");
    }

}
