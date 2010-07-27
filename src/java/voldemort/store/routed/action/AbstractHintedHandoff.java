package voldemort.store.routed.action;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.Store;
import voldemort.store.UnreachableStoreException;
import voldemort.store.routed.BasicPipelineData;
import voldemort.store.routed.Pipeline;
import voldemort.store.slop.Slop;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;
import voldemort.utils.Utils;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public abstract class AbstractHintedHandoff<V, PD extends BasicPipelineData<V>> extends
                                                                                AbstractKeyBasedAction<ByteArray, V, PD> {

    private final FailureDetector failureDetector;

    private final Map<Integer, Store<ByteArray, Slop>> slopStores;

    private final List<Node> nodes;

    protected final List<Node> failedNodes;
    
    public AbstractHintedHandoff(PD pipelineData,
                                 Pipeline.Event completeEvent,
                                 ByteArray key,
                                 FailureDetector failureDetector,
                                 Map<Integer, Store<ByteArray, Slop>> slopStores,
                                 Cluster cluster) {
        super(pipelineData, completeEvent, key);
        this.failureDetector = failureDetector;
        this.slopStores = slopStores;
        nodes = new ArrayList<Node>(cluster.getNodes());
        failedNodes = pipelineData.getFailedNodes();

        // shuffle potential slop nodes to avoid cascading failures
        Collections.shuffle(nodes, new Random());
    }

     protected boolean handoffSlop(Node failedNode, Version version, Slop slop) {
        Set<Node> used = new HashSet<Node>(nodes.size());
        boolean persisted = false;
        for(Node node: nodes) {
            int nodeId = node.getId();
            Store<ByteArray, Slop> slopStore = slopStores.get(nodeId);

            if(!failedNodes.contains(node) && failureDetector.isAvailable(node)) {
                Utils.notNull(slopStore);
                long startNs = System.nanoTime();

                try {
                    if(logger.isTraceEnabled())
                        logger.trace("Attempt to write " + slop.getKey() + " for "
                                     + failedNode + " to node " + node);

                    slopStore.put(slop.makeKey(),
                                  new Versioned<Slop>(slop, version));

                    persisted = true;
                    failureDetector.recordSuccess(node,
                                                  (System.nanoTime() - startNs) / Time.NS_PER_MS);
                    used.add(node);

                    if(logger.isTraceEnabled())
                        logger.trace("Finished hinted handoff for " + failedNode
                                     + " wrote slop to " + node);
                    break;
                } catch(UnreachableStoreException e) {
                    failureDetector.recordException(node,
                                                    (System.nanoTime() - startNs) / Time.NS_PER_MS,
                                                    e);
                    logger.warn("Error during hinted handoff", e);
                }
            }
        }

        if(nodes.size() > used.size())
            for(Node usedNode: used)
                nodes.remove(usedNode);
        return persisted;
    }
    
    public abstract void execute(Pipeline pipeline);
}
