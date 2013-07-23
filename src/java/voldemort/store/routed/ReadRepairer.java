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

package voldemort.store.routed;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import voldemort.annotations.concurrency.Threadsafe;
import voldemort.versioning.Occurred;
import voldemort.versioning.Version;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

/**
 * Repair out-dated reads, by sending an up-to-date value back to the offending
 * clients
 * 
 * This class computes the set of repairs that need to be made.
 * 
 * 
 * @param <K> The class of the key in the fetch
 * @param <V> The class of the value in the fetch
 */
@Threadsafe
public class ReadRepairer<K, V> {

    private final Logger logger = Logger.getLogger(getClass());

    /**
     * Compute the repair set from the given values and nodes
     * 
     * @param nodeValues The value found on each node
     * @return A set of repairs to perform
     */
    public List<NodeValue<K, V>> getRepairs(List<NodeValue<K, V>> nodeValues) {
        int size = nodeValues.size();
        if(size <= 1)
            return Collections.emptyList();

        Map<K, List<NodeValue<K, V>>> keyToNodeValues = Maps.newHashMap();
        for(NodeValue<K, V> nodeValue: nodeValues) {
            List<NodeValue<K, V>> keyNodeValues = keyToNodeValues.get(nodeValue.getKey());
            if(keyNodeValues == null) {
                keyNodeValues = Lists.newArrayListWithCapacity(5);
                keyToNodeValues.put(nodeValue.getKey(), keyNodeValues);
            }
            keyNodeValues.add(nodeValue);
        }

        List<NodeValue<K, V>> result = Lists.newArrayList();
        for(List<NodeValue<K, V>> keyNodeValues: keyToNodeValues.values())
            result.addAll(singleKeyGetRepairs(keyNodeValues));
        return result;
    }

    private List<NodeValue<K, V>> singleKeyGetRepairs(List<NodeValue<K, V>> nodeValues) {
        int size = nodeValues.size();
        if(size <= 1)
            return Collections.emptyList();

        // 1. Create a multi-map of nodes to their existing Versions
        Multimap<Integer, NodeValue<K, V>> nodeVersionsMap = HashMultimap.create();
        for(NodeValue<K, V> nodeValue: nodeValues) {
            nodeVersionsMap.put(nodeValue.getNodeId(), nodeValue);
        }

        // 2. Create a map of the final set of versions (for this key)
        Map<Version, NodeValue<K, V>> mostCurrentVersionsMap = new HashMap<Version, NodeValue<K, V>>();

        // Initialize with the first element from the input
        mostCurrentVersionsMap.put(nodeValues.get(0).getVersion(), nodeValues.get(0));

        // check each value against the current set of most current versions
        for(int i = 1; i < nodeValues.size(); i++) {
            NodeValue<K, V> curr = nodeValues.get(i);
            boolean concurrentToAll = true;

            /*
             * Make a copy for the traversal. This is because the original map
             * can be modified during this traversal
             */
            Set<Version> knownGoodVersions = new HashSet<Version>(mostCurrentVersionsMap.keySet());

            for(Version currentGoodversion: knownGoodVersions) {

                // If the version already exists, do nothing
                if(curr.getVersion().equals(currentGoodversion)) {
                    concurrentToAll = false;
                    if(logger.isDebugEnabled()) {
                        logger.debug("Version already exists in the most current set: " + curr);
                    }
                    break;
                }

                // Check the ordering of the current value
                Occurred occurred = curr.getVersion().compare(currentGoodversion);
                if(occurred == Occurred.BEFORE) {
                    // This value is obsolete! Break from the loop
                    if(logger.isDebugEnabled()) {
                        logger.debug("Version is obsolete : " + curr);
                    }
                    concurrentToAll = false;
                    break;
                } else if(occurred == Occurred.AFTER) {
                    // This concurrent value is obsolete and the current value
                    // should replace it
                    mostCurrentVersionsMap.remove(currentGoodversion);
                    concurrentToAll = false;
                    mostCurrentVersionsMap.put(curr.getVersion(), curr);
                    if(logger.isDebugEnabled()) {
                        logger.debug("Updating the current best - adding : " + curr);
                    }
                }
            }
            // if the value is concurrent to all existing versions then add it
            // to the concurrent set
            if(concurrentToAll) {
                mostCurrentVersionsMap.put(curr.getVersion(), curr);
                if(logger.isDebugEnabled()) {
                    logger.debug("Value is concurrent to all ! : " + curr);
                }
            }
        }

        // 3. Compare 1 and 2 and create the repair list
        List<NodeValue<K, V>> repairs = new ArrayList<NodeValue<K, V>>(3);
        for(int nodeId: nodeVersionsMap.keySet()) {
            Set<Version> finalVersions = new HashSet<Version>(mostCurrentVersionsMap.keySet());
            if(logger.isDebugEnabled()) {
                logger.debug("Set of final versions = " + finalVersions);
            }

            // Calculate the set difference between final Versions and
            // the versions currently existing for nodeId
            Set<Version> currentNodeVersions = new HashSet<Version>();
            for(NodeValue<K, V> nodeValue: nodeVersionsMap.get(nodeId)) {
                currentNodeVersions.add(nodeValue.getVersion());
            }
            finalVersions.removeAll(currentNodeVersions);

            if(logger.isDebugEnabled()) {
                logger.debug("Remaining versions to be repaired for this node after the set difference = "
                             + finalVersions);
            }

            // Repair nodeId with the remaining Versioned values
            for(Version remainingVersion: finalVersions) {
                NodeValue<K, V> repair = new NodeValue<K, V>(nodeId,
                                                             mostCurrentVersionsMap.get(remainingVersion)
                                                                                   .getKey(),
                                                             mostCurrentVersionsMap.get(remainingVersion)
                                                                                   .getVersioned());
                if(logger.isDebugEnabled()) {
                    logger.debug("Node value marked to be repaired : " + repair);
                }
                repairs.add(repair);
            }
        }

        return repairs;
    }
}
