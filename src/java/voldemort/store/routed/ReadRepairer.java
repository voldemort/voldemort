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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

        // A list of obsolete nodes that need to be repaired
        Set<Integer> obsolete = new HashSet<Integer>(3);

        // A Map of Version=>NodeValues that contains the current best estimate
        // of the set of current versions
        // and the nodes containing them
        Multimap<Version, NodeValue<K, V>> concurrents = HashMultimap.create();
        concurrents.put(nodeValues.get(0).getVersion(), nodeValues.get(0));

        // check each value against the current set of most current versions
        for(int i = 1; i < nodeValues.size(); i++) {
            NodeValue<K, V> curr = nodeValues.get(i);
            boolean concurrentToAll = true;
            Set<Version> versions = new HashSet<Version>(concurrents.keySet());
            for(Version concurrentVersion: versions) {

                // if we already have the version, just add the nodevalue for
                // future updating and move on
                if(curr.getVersion().equals(concurrentVersion)) {
                    concurrents.put(curr.getVersion(), curr);
                    break;
                }

                // Check the ordering of the current value
                Occurred occurred = curr.getVersion().compare(concurrentVersion);
                if(occurred == Occurred.BEFORE) {
                    // This value is obsolete! Stop checking against other
                    // values...
                    obsolete.add(curr.getNodeId());
                    concurrentToAll = false;
                    break;
                } else if(occurred == Occurred.AFTER) {
                    // This concurrent value is obsolete and the current value
                    // should replace it
                    for(NodeValue<K, V> v: concurrents.get(concurrentVersion))
                        obsolete.add(v.getNodeId());
                    concurrents.removeAll(concurrentVersion);
                    concurrentToAll = false;
                    concurrents.put(curr.getVersion(), curr);
                }
            }
            // if the value is concurrent to all existing versions then add it
            // to the concurrent set
            if(concurrentToAll)
                concurrents.put(curr.getVersion(), curr);
        }

        // Construct the list of repairs
        List<NodeValue<K, V>> repairs = new ArrayList<NodeValue<K, V>>(3);
        for(Integer id: obsolete) {
            // repair all obsolete nodes
            for(Version v: concurrents.keySet()) {
                NodeValue<K, V> concurrent = concurrents.get(v).iterator().next();
                NodeValue<K, V> repair = new NodeValue<K, V>(id,
                                                             concurrent.getKey(),
                                                             concurrent.getVersioned());
                repairs.add(repair);
            }
        }

        if(concurrents.size() > 1) {
            // if there are more then one concurrent versions on different
            // nodes,
            // we should repair so all have the same set of values
            Set<NodeValue<K, V>> existing = new HashSet<NodeValue<K, V>>(repairs);
            for(NodeValue<K, V> entry1: concurrents.values()) {
                for(NodeValue<K, V> entry2: concurrents.values()) {
                    if(!entry1.getVersion().equals(entry2.getVersion())) {
                        NodeValue<K, V> repair = new NodeValue<K, V>(entry1.getNodeId(),
                                                                     entry2.getKey(),
                                                                     entry2.getVersioned());
                        if(!existing.contains(repair))
                            repairs.add(repair);
                    }
                }
            }
        }

        return repairs;
    }

}
