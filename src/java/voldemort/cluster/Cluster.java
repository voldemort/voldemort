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

package voldemort.cluster;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.annotations.concurrency.Threadsafe;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxManaged;
import voldemort.utils.Utils;

import com.google.common.collect.Sets;

/**
 * A representation of the voldemort cluster
 * 
 * 
 */
@Threadsafe
@JmxManaged(description = "Metadata about the physical servers on which the Voldemort cluster runs")
public class Cluster implements Serializable {

    private static final long serialVersionUID = 1;

    private final String name;
    private final int numberOfTags;
    private final Map<Integer, Node> nodesById;
    private final Map<Integer, Zone> zonesById;

    public Cluster(String name, List<Node> nodes) {
        this(name, nodes, new ArrayList<Zone>());
    }

    public Cluster(String name, List<Node> nodes, List<Zone> zones) {
        this.name = Utils.notNull(name);
        if(zones.size() != 0) {
            zonesById = new LinkedHashMap<Integer, Zone>(zones.size());
            for(Zone zone: zones) {
                if(zonesById.containsKey(zone.getId()))
                    throw new IllegalArgumentException("Zone id " + zone.getId()
                                                       + " appears twice in the zone list.");
                zonesById.put(zone.getId(), zone);
            }
        } else {
            // Add default zone
            zonesById = new LinkedHashMap<Integer, Zone>(1);
            zonesById.put(Zone.DEFAULT_ZONE_ID, new Zone());
        }

        this.nodesById = new LinkedHashMap<Integer, Node>(nodes.size());
        for(Node node: nodes) {
            if(nodesById.containsKey(node.getId()))
                throw new IllegalArgumentException("Node id " + node.getId()
                                                   + " appears twice in the node list.");
            nodesById.put(node.getId(), node);
        }
        this.numberOfTags = getNumberOfTags(nodes);
    }

    private int getNumberOfTags(List<Node> nodes) {
        List<Integer> tags = new ArrayList<Integer>();
        for(Node node: nodes)
            tags.addAll(node.getPartitionIds());
        Collections.sort(tags);
        for(int i = 0; i < numberOfTags; i++) {
            if(tags.get(i).intValue() != i)
                throw new IllegalArgumentException("Invalid tag assignment.");
        }
        return tags.size();
    }

    @JmxGetter(name = "name", description = "The name of the cluster")
    public String getName() {
        return name;
    }

    public Collection<Node> getNodes() {
        return nodesById.values();
    }

    public Collection<Zone> getZones() {
        return zonesById.values();
    }

    public Zone getZoneById(int id) {
        Zone zone = zonesById.get(id);
        if(zone == null)
            throw new VoldemortException("No such zone in cluster: " + id);
        return zone;
    }

    public int getNumberOfZones() {
        return zonesById.size();
    }

    public Node getNodeById(int id) {
        Node node = nodesById.get(id);
        if(node == null)
            throw new VoldemortException("No such node in cluster: " + id);
        return node;
    }

    @JmxGetter(name = "numberOfNodes", description = "The number of nodes in the cluster.")
    public int getNumberOfNodes() {
        return nodesById.size();
    }

    public int getNumberOfPartitions() {
        return numberOfTags;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Cluster('");
        builder.append(getName());
        builder.append("', [");
        for(Node n: getNodes()) {
            builder.append(n.toString());
            builder.append(", ");
        }
        builder.append("])");

        return builder.toString();
    }

    @Override
    public boolean equals(Object second) {
        if(this == second)
            return true;
        if(second == null || second.getClass() != getClass())
            return false;

        Cluster secondCluster = (Cluster) second;
        if(this.getNodes().size() != secondCluster.getNodes().size()) {
            return false;
        }
        for(Node nodeA: this.getNodes()) {
            Node nodeB = secondCluster.getNodeById(nodeA.getId());

            if(nodeA.getNumberOfPartitions() != nodeB.getNumberOfPartitions()) {
                return false;
            }

            if(!Sets.newHashSet(nodeA.getPartitionIds())
                    .equals(Sets.newHashSet(nodeB.getPartitionIds())))
                return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int hc = getNodes().size();
        for(Node node: getNodes()) {
            hc ^= node.getHost().hashCode();
        }

        return hc;
    }
}
