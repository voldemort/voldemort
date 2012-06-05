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

/*
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
