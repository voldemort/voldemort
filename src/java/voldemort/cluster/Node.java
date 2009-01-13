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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import voldemort.annotations.concurrency.Threadsafe;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

/**
 * A node in the voldemort cluster
 * 
 * @author jay
 * 
 */
@Threadsafe
public class Node implements Serializable {

    private static final long serialVersionUID = 1;

    private final int id;
    private final String host;
    private final int httpPort;
    private final int socketPort;
    private final List<Integer> partitions;
    private NodeStatus status;

    public Node(int id, String host, int httpPort, int socketPort, List<Integer> partitions) {
        this(id, host, httpPort, socketPort, partitions, new NodeStatus());
    }

    public Node(int id,
                String host,
                int httpPort,
                int socketPort,
                List<Integer> partitions,
                NodeStatus status) {
        this.id = id;
        this.host = Objects.nonNull(host);
        this.httpPort = httpPort;
        this.socketPort = socketPort;
        this.status = status;
        this.partitions = ImmutableList.copyOf(partitions);
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

    public int getId() {
        return id;
    }

    public NodeStatus getStatus() {
        return status;
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
            throw new IllegalStateException("Invalid host format for node " + id + ".");
        }
    }

    public URI getSocketUrl() {
        try {
            return new URI("tcp://" + getHost() + ":" + getSocketPort());
        } catch(URISyntaxException e) {
            throw new IllegalStateException("Invalid host format for node " + id + ".");
        }
    }

    @Override
    public String toString() {
        return "Node" + getId();
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

}
