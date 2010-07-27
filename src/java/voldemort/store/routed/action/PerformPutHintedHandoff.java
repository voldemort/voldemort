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
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import java.util.Date;
import java.util.Map;

public class PerformPutHintedHandoff extends
                                     AbstractHintedHandoff<Void, PutPipelineData> {

    private final Versioned<byte[]> versioned;

    private final Time time;

    public PerformPutHintedHandoff(PutPipelineData pipelineData,
                                   Pipeline.Event completeEvent,
                                   ByteArray key,
                                   Versioned<byte[]> versioned,
                                   FailureDetector failureDetector,
                                   Map<Integer, Store<ByteArray, Slop>> slopStores,
                                   Cluster cluster,
                                   Time time) {
        super(pipelineData, completeEvent, key, failureDetector, slopStores, cluster);
        this.versioned = versioned;
        this.time = time;
    }

    @Override
    public void execute(Pipeline pipeline) {
        Versioned<byte[]> versionedCopy = pipelineData.getVersionedCopy();
        for(Node failedNode: failedNodes) {
            int failedNodeId = failedNode.getId();
            if(versionedCopy == null) {
                VectorClock clock = (VectorClock) versioned.getVersion();
                versionedCopy = new Versioned<byte[]>(versioned.getValue(),
                                                      clock.incremented(failedNodeId,
                                                                        time.getMilliseconds()));
            }

            Version version = versionedCopy.getVersion();
            if(logger.isTraceEnabled())
                logger.trace("Performing hinted handoff for node " + failedNode + ", store "
                             + pipelineData.getStoreName() + " key " + key + ", version "
                             + version);

            Slop slop = new Slop(pipelineData.getStoreName(),
                                 Slop.Operation.PUT,
                                 key,
                                 versionedCopy.getValue(),
                                 failedNodeId,
                                 new Date());

            boolean persisted = handoffSlop(failedNode, version, slop);

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
