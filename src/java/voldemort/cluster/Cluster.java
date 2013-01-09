/*
 * Copyright 2008-2013 LinkedIn, Inc
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
import java.util.Set;
import java.util.TreeSet;

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
    private final Map<Zone, List<Integer>> nodesPerZone;
    private final Map<Zone, List<Integer>> partitionsPerZone;

    public Cluster(String name, List<Node> nodes) {
        this(name, nodes, new ArrayList<Zone>());
    }

    public Cluster(String name, List<Node> nodes, List<Zone> zones) {
        this.name = Utils.notNull(name);
        this.partitionsPerZone = new LinkedHashMap<Zone, List<Integer>>();
        this.nodesPerZone = new LinkedHashMap<Zone, List<Integer>>();

        if(zones.size() != 0) {
            zonesById = new LinkedHashMap<Integer, Zone>(zones.size());
            for(Zone zone: zones) {
                if(zonesById.containsKey(zone.getId()))
                    throw new IllegalArgumentException("Zone id " + zone.getId()
                                                       + " appears twice in the zone list.");
                zonesById.put(zone.getId(), zone);
                nodesPerZone.put(zone, new ArrayList<Integer>());
                partitionsPerZone.put(zone, new ArrayList<Integer>());
            }
        } else {
            // Add default zone
            zonesById = new LinkedHashMap<Integer, Zone>(1);
            Zone defaultZone = new Zone();
            zonesById.put(defaultZone.getId(), defaultZone);
            nodesPerZone.put(defaultZone, new ArrayList<Integer>());
            partitionsPerZone.put(defaultZone, new ArrayList<Integer>());
        }

        this.nodesById = new LinkedHashMap<Integer, Node>(nodes.size());
        for(Node node: nodes) {
            if(nodesById.containsKey(node.getId()))
                throw new IllegalArgumentException("Node id " + node.getId()
                                                   + " appears twice in the node list.");
            nodesById.put(node.getId(), node);

            Zone nodesZone = zonesById.get(node.getZoneId());
            nodesPerZone.get(nodesZone).add(node.getId());
            partitionsPerZone.get(nodesZone).addAll(node.getPartitionIds());
        }
        this.numberOfTags = getNumberOfTags(nodes);
    }

    private int getNumberOfTags(List<Node> nodes) {
        List<Integer> tags = new ArrayList<Integer>();
        for(Node node: nodes) {
            tags.addAll(node.getPartitionIds());
        }
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

    /**
     * @return Sorted set of node Ids
     */
    public Set<Integer> getNodeIds() {
        Set<Integer> nodeIds = nodesById.keySet();
        return new TreeSet<Integer>(nodeIds);
    }

    /**
     * 
     * @return Sorted set of Zone Ids
     */
    public Set<Integer> getZoneIds() {
        Set<Integer> zoneIds = zonesById.keySet();
        return new TreeSet<Integer>(zoneIds);
    }

    public Collection<Zone> getZones() {
        return zonesById.values();
    }

    public Zone getZoneById(int id) {
        Zone zone = zonesById.get(id);
        if(zone == null) {
            if(id == Zone.DEFAULT_ZONE_ID)
                throw new VoldemortException("Incorrect configuration. Default zone ID:" + id
                                             + " required but not specified.");
            else {
                throw new VoldemortException("No such zone in cluster: " + id
                                             + " Available zones : " + displayZones());
            }

        }
        return zone;
    }

    private String displayZones() {
        String zoneIDS = "{";
        for(Zone z: this.getZones()) {
            if(zoneIDS.length() != 1)
                zoneIDS += ",";
            zoneIDS += z.getId();
        }
        zoneIDS += "}";
        return zoneIDS;
    }

    public int getNumberOfZones() {
        return zonesById.size();
    }

    public int getNumberOfPartitionsInZone(Integer zoneId) {
        return partitionsPerZone.get(getZoneById(zoneId)).size();
    }

    public int getNumberOfNodesInZone(Integer zoneId) {
        return nodesPerZone.get(getZoneById(zoneId)).size();
    }

    /**
     * @return Sorted set of node Ids for given zone
     */
    public Set<Integer> getNodeIdsInZone(Integer zoneId) {
        return new TreeSet<Integer>(nodesPerZone.get(getZoneById(zoneId)));
    }

    /**
     * @return Sorted set of partition Ids for given zone
     */
    public Set<Integer> getPartitionIdsInZone(Integer zoneId) {
        return new TreeSet<Integer>(partitionsPerZone.get(getZoneById(zoneId)));
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
            builder.append('\n');
        }
        builder.append("])");

        return builder.toString();
    }

    /**
     * Return a detailed string representation of the current cluster
     * 
     * @param isDetailed
     * @return
     */
    public String toString(boolean isDetailed) {
        if(!isDetailed) {
            return toString();
        }
        StringBuilder builder = new StringBuilder("Cluster [" + getName() + "] Nodes ["
                                                  + getNumberOfNodes() + "] Zones ["
                                                  + getNumberOfZones() + "] Partitions ["
                                                  + getNumberOfPartitions() + "]");
        builder.append(" Zone Info [" + getZones() + "]");
        builder.append(" Node Info [" + getNodes() + "]");
        return builder.toString();
    }

    @Override
    public boolean equals(Object second) {
        if(this == second)
            return true;
        if(second == null || second.getClass() != getClass())
            return false;

        Cluster secondCluster = (Cluster) second;
        if(this.getZones().size() != secondCluster.getZones().size()) {
            return false;
        }

        if(this.getNodes().size() != secondCluster.getNodes().size()) {
            return false;
        }

        for(Zone zoneA: this.getZones()) {
            Zone zoneB;
            try {
                zoneB = secondCluster.getZoneById(zoneA.getId());
            } catch(VoldemortException e) {
                return false;
            }
            if(zoneB == null || zoneB.getProximityList().size() != zoneA.getProximityList().size()) {
                return false;
            }

            for(int index = 0; index < zoneA.getProximityList().size(); index++) {
                if(zoneA.getProximityList().get(index) != zoneB.getProximityList().get(index)) {
                    return false;
                }
            }
        }
        for(Node nodeA: this.getNodes()) {
            Node nodeB;
            try {
                nodeB = secondCluster.getNodeById(nodeA.getId());
            } catch(VoldemortException e) {
                return false;
            }
            if(nodeA.getNumberOfPartitions() != nodeB.getNumberOfPartitions()) {
                return false;
            }

            if(nodeA.getZoneId() != nodeB.getZoneId()) {
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
