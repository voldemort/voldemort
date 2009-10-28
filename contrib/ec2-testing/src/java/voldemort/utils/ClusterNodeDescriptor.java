/*
 * Copyright 2009 LinkedIn, Inc.
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

package voldemort.utils;

import java.util.List;

/**
 * ClusterNodeDescriptor is a simple POJO for storing the attributes of a node
 * as needed by the cluster.xml cluster descriptor file.
 * 
 * @author Kirk True
 * 
 * @see ClusterGenerator
 */

public class ClusterNodeDescriptor {

    /**
     * DEFAULT_HTTP_PORT is 8081 as seen in the examples.
     */

    public static final int DEFAULT_HTTP_PORT = 8081;

    /**
     * DEFAULT_SOCKET_PORT is 6666 as seen in the examples.
     */

    public static final int DEFAULT_SOCKET_PORT = 6666;

    private String hostName;

    private int id;

    private int httpPort = DEFAULT_HTTP_PORT;

    private int socketPort = DEFAULT_SOCKET_PORT;

    private List<Integer> partitions;

    /**
     * Returns the host name (or IP address) of the node. This is the internal
     * host name as seen by the other server nodes and clients on the same
     * network.
     * 
     * @return Host name (or IP address) or null if unset
     */

    public String getHostName() {
        return hostName;
    }

    /**
     * Assigns the host name (or IP address) of the node. This is the internal
     * host name as seen by the other server nodes and clients on the same
     * network.
     * 
     * @param hostName Host name (or IP address)
     */

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    /**
     * Returns the ID of the node. This is the node ID that is used in both the
     * cluster.xml and server.properties configuration files.
     * 
     * @return Node ID
     */

    public int getId() {
        return id;
    }

    /**
     * Assigns the ID of the node. This is the node ID that is used in both the
     * cluster.xml and server.properties configuration files.
     * 
     * @param id Node ID
     */

    public void setId(int id) {
        this.id = id;
    }

    /**
     * Retrieves the port used by the HTTP server. It defaults to
     * DEFAULT_HTTP_PORT if left unset.
     * 
     * @return Port used by the HTTP server
     */

    public int getHttpPort() {
        return httpPort;
    }

    /**
     * Assigns the port used by the HTTP server.
     * 
     * @param httpPort Port used by the HTTP server
     */

    public void setHttpPort(int httpPort) {
        this.httpPort = httpPort;
    }

    /**
     * Retrieves the port used by the socket server. It defaults to
     * DEFAULT_SOCKET_PORT if left unset.
     * 
     * @return Port used by the socket server
     */

    public int getSocketPort() {
        return socketPort;
    }

    /**
     * Assigns the port used by the socket server. It defaults to
     * DEFAULT_SOCKET_PORT if left unset.
     * 
     * @param socketPort Port used by the socket server
     */

    public void setSocketPort(int socketPort) {
        this.socketPort = socketPort;
    }

    /**
     * Returns the list of partition IDs used by this particular node.
     * 
     * @return List of partitions, or null if unset
     */

    public List<Integer> getPartitions() {
        return partitions;
    }

    /**
     * Assigns the list of partition IDs used by this particular node. Please
     * ensure that the partitions provided to <i>this</i> node have no overlap
     * with any partitions for any <i>other</i> nodes in the cluster. This class
     * doesn't enforce/check overlaps; you'll find them at runtime ;)
     * 
     * <p>
     * 
     * Also, if possible, provide the list in ascending sorted order. (This is
     * an optimization for humans.)
     * 
     * @param partitions List of partitions
     */

    public void setPartitions(List<Integer> partitions) {
        this.partitions = partitions;
    }

}
