package voldemort.store.routed.action;

import java.util.List;

import voldemort.VoldemortException;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.routing.RoutingStrategy;
import voldemort.store.routed.BasicPipelineData;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Pipeline.Event;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;

/*
 * Default Configure Nodes that does not reorder the list of nodes obtained via
 * the routing strategy
 */
public class ConfigureNodesDefault<V, PD extends BasicPipelineData<V>> extends
        AbstractConfigureNodes<ByteArray, V, PD> {

    private final ByteArray key;

    public ConfigureNodesDefault(PD pipelineData,
                                 Event completeEvent,
                                 FailureDetector failureDetector,
                                 int required,
                                 RoutingStrategy routingStrategy,
                                 ByteArray key) {
        super(pipelineData, completeEvent, failureDetector, required, routingStrategy);
        this.key = key;
    }

    @Override
    public List<Node> getNodes(ByteArray key) {
        List<Node> nodes = null;

        try {
            nodes = super.getNodes(key);
        } catch(VoldemortException e) {
            pipelineData.setFatalError(e);
            return null;
        }
        return nodes;
    }

    public void execute(Pipeline pipeline) {
        List<Node> nodes = null;

        nodes = getNodes(key);
        if(nodes == null) {
            pipeline.abort();
            return;
        }

        if(logger.isDebugEnabled())
            logger.debug("Adding " + nodes.size() + " node(s) to preference list");

        if(logger.isDebugEnabled()) {
            StringBuilder nodeStr = new StringBuilder();
            for(Node node: nodes) {
                nodeStr.append(node.getId() + ",");
            }
            logger.debug("Key " + ByteUtils.toHexString(key.get())
                         + " final preference list to contact " + nodeStr);
        }
        pipelineData.setNodes(nodes);
        pipeline.addEvent(completeEvent);
    }

}
