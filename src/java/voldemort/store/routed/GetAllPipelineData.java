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

import java.util.List;
import java.util.Map;

import voldemort.cluster.Node;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

public class GetAllPipelineData extends PipelineData<Map<ByteArray, List<Versioned<byte[]>>>> {

    // Keys for each node needed to satisfy storeDef.getPreferredReads() if
    // no failures.
    private Map<Node, List<ByteArray>> nodeToKeysMap;

    // Keep track of nodes per key that might be needed if there are
    // failures during getAll
    private Map<ByteArray, List<Node>> keyToExtraNodesMap;

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

}
