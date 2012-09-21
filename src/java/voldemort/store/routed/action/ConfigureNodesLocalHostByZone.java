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

package voldemort.store.routed.action;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import voldemort.VoldemortException;
import voldemort.cluster.Node;
import voldemort.cluster.Zone;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.routing.RoutingStrategy;
import voldemort.store.routed.BasicPipelineData;
import voldemort.store.routed.Pipeline.Event;
import voldemort.store.routed.Pipeline.Operation;
import voldemort.utils.ByteArray;

/**
 * Use the zone aware node list returned via the routing strategy. However give
 * preference to the current node, if it is part of the preflist returned from
 * the routing strategy.
 */

public class ConfigureNodesLocalHostByZone<V, PD extends BasicPipelineData<V>> extends
        ConfigureNodesByZone<V, PD> {

    public ConfigureNodesLocalHostByZone(PD pipelineData,
                                         Event completeEvent,
                                         FailureDetector failureDetector,
                                         int required,
                                         RoutingStrategy routingStrategy,
                                         ByteArray key,
                                         Zone clientZone) {
        super(pipelineData,
              completeEvent,
              failureDetector,
              required,
              routingStrategy,
              key,
              clientZone);
    }

    /*
     * If the current node exists in the nodes list, bring it to the front
     */
    @Override
    public List<Node> getNodes(ByteArray key, Operation op) {
        List<Node> nodes = null;
        List<Node> reorderedNodes = new ArrayList<Node>();

        try {
            nodes = super.getNodes(key, op);
            String currentHost = InetAddress.getLocalHost().getHostName();
            for(Node n: nodes) {
                if(currentHost.contains(n.getHost()) || n.getHost().contains(currentHost)) {
                    reorderedNodes.add(n);
                    nodes.remove(n);
                    break;
                }
            }
            reorderedNodes.addAll(nodes);
            nodes = reorderedNodes;
        } catch(VoldemortException e) {
            pipelineData.setFatalError(e);
            return null;
        } catch(UnknownHostException e) {
            e.printStackTrace();
            return null;
        }
        return nodes;
    }
}
