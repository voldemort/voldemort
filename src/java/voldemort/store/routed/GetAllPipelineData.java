/*
 * Copyright 2010 LinkedIn, Inc
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

package voldemort.store.routed;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.mutable.MutableInt;

import voldemort.cluster.Node;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

/**
 * This is used only by the "get all" operation as it includes data specific
 * only to that operation.
 */

public class GetAllPipelineData extends
        PipelineData<Iterable<ByteArray>, Map<ByteArray, List<Versioned<byte[]>>>> {

    private final Map<ByteArray, List<Versioned<byte[]>>> result;

    // Keys for each node needed to satisfy storeDef.getPreferredReads() if
    // no failures.
    private Map<Node, List<ByteArray>> nodeToKeysMap;

    // Keep track of nodes per key that might be needed if there are
    // failures during getAll
    private Map<ByteArray, List<Node>> keyToExtraNodesMap;

    private final Map<ByteArray, MutableInt> keyToSuccessCount;

    private final Map<ByteArray, HashSet<Integer>> keyToZoneResponses;

    private Map<ByteArray, byte[]> transforms;

    private Integer zonesRequired;

    public GetAllPipelineData() {
        this.result = new HashMap<ByteArray, List<Versioned<byte[]>>>();
        this.keyToSuccessCount = new HashMap<ByteArray, MutableInt>();
        this.keyToZoneResponses = new HashMap<ByteArray, HashSet<Integer>>();
    }

    public Map<ByteArray, HashSet<Integer>> getKeyToZoneResponse() {
        return this.keyToZoneResponses;
    }

    public Map<ByteArray, List<Versioned<byte[]>>> getResult() {
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

    public Map<ByteArray, byte[]> getTransforms() {
        return transforms;
    }

    public void setTransforms(Map<ByteArray, byte[]> transforms) {
        this.transforms = transforms;
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
