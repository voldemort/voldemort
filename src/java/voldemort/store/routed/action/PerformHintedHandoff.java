package voldemort.store.routed.action;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.Store;
import voldemort.store.UnreachableStoreException;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.PutPipelineData;
import voldemort.store.slop.Slop;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;
import voldemort.utils.Utils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class PerformHintedHandoff extends
                                  AbstractKeyBasedAction<ByteArray, Void, PutPipelineData> {

    private final Versioned<byte[]> versioned;

    private final FailureDetector failureDetector;

    private final Map<Integer, Store<ByteArray, Slop>> slopStores;

    private final Cluster cluster;

    private final Time time;

    private final Random rand;

    public PerformHintedHandoff(PutPipelineData pipelineData,
                                Pipeline.Event completeEvent,
                                ByteArray key,
                                Versioned<byte[]> versioned,
                                FailureDetector failureDetector,
                                Map<Integer, Store<ByteArray, Slop>> slopStores,
                                Cluster cluster,
                                Time time) {
        super(pipelineData, completeEvent, key);
        this.versioned = versioned;
        this.failureDetector = failureDetector;
        this.slopStores = slopStores;
        this.cluster = cluster;
        this.time = time;
        this.rand = new Random();
    }

    public void execute(Pipeline pipeline) {
        List<Node> nodes = new ArrayList<Node>(cluster.getNodes());

        // shuffle potential slop nodes to avoid cascading failures
        Collections.shuffle(nodes, rand);

        Versioned<byte[]> versionedCopy = pipelineData.getVersionedCopy();
        for(Node failedNode: pipelineData.getFailedNodes()) {
            int failedNodeId = failedNode.getId();
            if(versionedCopy == null) {
                VectorClock clock = (VectorClock) versioned.getVersion();
                versionedCopy = new Versioned<byte[]>(versioned.getValue(),
                                                      clock.incremented(failedNodeId,
                                                                        time.getMilliseconds()));
            }

            if(logger.isTraceEnabled())
                logger.trace("Performing hinted handoff for node " + failedNode + ", store "
                             + pipelineData.getStoreName() + " key " + key + ", version "
                             + versionedCopy.getVersion());

            Slop slop = new Slop(pipelineData.getStoreName(),
                                 Slop.Operation.PUT,
                                 key,
                                 versionedCopy.getValue(),
                                 failedNodeId,
                                 new Date());

            Set<Node> used = new HashSet<Node>(nodes.size());
            boolean persisted = false;
            for(Node node: nodes) {
                int nodeId = node.getId();
                Store<ByteArray, Slop> slopStore = slopStores.get(nodeId);

                if(nodeId != failedNode.getId() && failureDetector.isAvailable(node)) {
                    Utils.notNull(slopStore);
                    long startNs = System.nanoTime();

                    try {
                        if(logger.isTraceEnabled())
                            logger.trace("Attempt to write " + slop.getKey() + " for "
                                         + failedNode + " to node " + node);

                        slopStore.put(slop.makeKey(),
                                      new Versioned<Slop>(slop, versionedCopy.getVersion()));
                        
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

            Exception e = pipelineData.getFatalError();
            if(e != null) {
                if(persisted)
                  pipelineData.setFatalError(new UnreachableStoreException("Put operation failed on node "
                                                                             + failedNodeId
                                                                             + ", but has been persisted to slop storage for eventual replication.",
                                                                             e));
                else
                    pipelineData.setFatalError(new InsufficientOperationalNodesException("All slop servers are unavailable from node "
                                                                                         + failedNodeId + ".",
                                                                                         e));
            }
        }

        pipeline.addEvent(completeEvent);
    }
}
