package voldemort.store.routed;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.mutable.MutableInt;

import voldemort.cluster.Node;
import voldemort.utils.ByteArray;

/**
 * This is used only by the "get all" operation as it includes data specific
 * only to that operation.
 */

public class HasKeysPipelineData extends PipelineData<Iterable<ByteArray>, Map<ByteArray, Boolean>> {

    private final Map<ByteArray, Boolean> result;

    // Keys for each node needed to satisfy storeDef.getPreferredReads() if
    // no failures.
    private Map<Node, List<ByteArray>> nodeToKeysMap;

    // Keep track of nodes per key that might be needed if there are
    // failures during getAll
    private Map<ByteArray, List<Node>> keyToExtraNodesMap;

    private final Map<ByteArray, MutableInt> keyToSuccessCount;

    private final Map<ByteArray, HashSet<Integer>> keyToZoneResponses;

    private Integer zonesRequired;

    public HasKeysPipelineData() {
        this.result = new HashMap<ByteArray, Boolean>();
        this.keyToSuccessCount = new HashMap<ByteArray, MutableInt>();
        this.keyToZoneResponses = new HashMap<ByteArray, HashSet<Integer>>();
    }

    public Map<ByteArray, HashSet<Integer>> getKeyToZoneResponse() {
        return this.keyToZoneResponses;
    }

    public Map<ByteArray, Boolean> getResult() {
        return result;
    }

    public Map<Node, List<ByteArray>> getNodeToKeysMap() {
        return nodeToKeysMap;
    }

    public void setNodeToKeysMap(Map<Node, List<ByteArray>> nodeToKeysMap) {
        this.nodeToKeysMap = nodeToKeysMap;
    }

    public Map<ByteArray, List<Node>> getKeyToExtraNodesMap() {
        return keyToExtraNodesMap;
    }

    public void setKeyToExtraNodesMap(Map<ByteArray, List<Node>> keyToExtraNodesMap) {
        this.keyToExtraNodesMap = keyToExtraNodesMap;
    }

    public MutableInt getSuccessCount(ByteArray key) {
        MutableInt value = keyToSuccessCount.get(key);

        if(value == null) {
            value = new MutableInt(0);
            keyToSuccessCount.put(key, value);
        }

        return value;
    }

    public void setZonesRequired(Integer zonesRequired) {
        this.zonesRequired = zonesRequired;
    }

    public Integer getZonesRequired() {
        return this.zonesRequired;
    }

}
