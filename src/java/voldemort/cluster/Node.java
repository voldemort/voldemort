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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.log4j.Logger;

import voldemort.annotations.concurrency.Threadsafe;
import voldemort.utils.Utils;

import com.google.common.collect.ImmutableList;

/**
 * A node in the voldemort cluster
 * 
 * 
 */
@Threadsafe
public class Node implements Serializable, Comparable<Node> {

    private static final Logger logger = Logger.getLogger(Node.class.getName());

    private static final long serialVersionUID = 1;
    private final int id;
    private final String host;
    private final int httpPort;
    private final int socketPort;
    private final int adminPort;
    private final int zoneId;
    private final List<Integer> partitions;

    public Node(int id,
                String host,
                int httpPort,
                int socketPort,
                int adminPort,
                List<Integer> partitions) {
        this(id, host, httpPort, socketPort, adminPort, Zone.DEFAULT_ZONE_ID, partitions);
    }

    public Node(int id,
                String host,
                int httpPort,
                int socketPort,
                int adminPort,
                int zoneId,
                List<Integer> partitions) {
        this.id = id;
        this.host = Utils.notNull(host);
        this.httpPort = httpPort;
        this.socketPort = socketPort;
        this.zoneId = zoneId;
        this.partitions = ImmutableList.copyOf(partitions);

        // fix default value for adminPort if not defined
        if(adminPort == -1) {
            adminPort = socketPort + 1;
            logger.warn("admin-port not defined for node:" + id
                        + " using default value(socket_port + 1):" + adminPort);
        }

        this.adminPort = adminPort;
    }

    public String getHost() {
        return host;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public int getSocketPort() {
        return socketPort;
    }

    public int getZoneId() {
        return zoneId;
    }

    public int getAdminPort() {
        return adminPort;
    }

    public int getId() {
        return id;
    }

    public List<Integer> getPartitionIds() {
        return partitions;
    }

    public int getNumberOfPartitions() {
        return partitions.size();
    }

    public URI getHttpUrl() {
        try {
            return new URI("http://" + getHost() + ":" + getHttpPort());
        } catch(URISyntaxException e) {
            throw new IllegalStateException("Invalid host format for node " + id + ".", e);
        }
    }

    public URI getSocketUrl() {
        try {
            return new URI("tcp://" + getHost() + ":" + getSocketPort());
        } catch(URISyntaxException e) {
            throw new IllegalStateException("Invalid host format for node " + id + ".", e);
        }
    }

    @Override
    public String toString() {
        return "Node " + getHost() + " Id:" + getId() + " in zone " + getZoneId()
               + " partitionList:" + partitions;
    }

    @Override
    public boolean equals(Object o) {
        if(this == o)
            return true;
        if(!(o instanceof Node))
            return false;

        Node n = (Node) o;
        return getId() == n.getId();
    }

    @Override
    public int hashCode() {
        return getId();
    }

    @Override
    public int compareTo(Node other) {
        return Integer.valueOf(this.id).compareTo(other.getId());
    }

    public boolean isEqualState(Node other) {
        return id == other.getId() && host.equalsIgnoreCase(other.getHost())
               && httpPort == other.getHttpPort() && socketPort == other.getSocketPort()
               && adminPort == other.getAdminPort() && zoneId == other.getZoneId();
    }
}