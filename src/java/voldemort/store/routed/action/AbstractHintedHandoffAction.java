package voldemort.store.routed.action;

import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.Store;
import voldemort.store.UnreachableStoreException;
import voldemort.store.routed.BasicPipelineData;
import voldemort.store.routed.HintedHandoff;
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

public abstract class AbstractHintedHandoffAction<V, PD extends BasicPipelineData<V>> extends
                                                                                      AbstractKeyBasedAction<ByteArray, V, PD> {

    protected final List<Node> failedNodes;

    protected final HintedHandoff hintedHandoff;
    
    public AbstractHintedHandoffAction(PD pipelineData,
                                       Pipeline.Event completeEvent,
                                       ByteArray key,
                                       FailureDetector failureDetector,
                                       Map<Integer, Store<ByteArray, Slop>> slopStores,
                                       Cluster cluster) {
        super(pipelineData, completeEvent, key);
        List<Node> nodes = new ArrayList<Node>(cluster.getNodes());
        failedNodes = pipelineData.getFailedNodes();
        hintedHandoff = new HintedHandoff(failureDetector,
                                          slopStores,
                                          nodes,
                                          failedNodes);
    }

    public abstract void execute(Pipeline pipeline);
}
