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

public class ClusterNodeDescriptor {

    private static final int DEFAULT_HTTP_PORT = 8081;

    private static final int DEFAULT_SOCKET_PORT = 6666;

    private String hostName;

    private int id;

    private int httpPort = DEFAULT_HTTP_PORT;

    private int socketPort = DEFAULT_SOCKET_PORT;

    private List<Integer> partitions;

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public void setHttpPort(int httpPort) {
        this.httpPort = httpPort;
    }

    public int getSocketPort() {
        return socketPort;
    }

    public void setSocketPort(int socketPort) {
        this.socketPort = socketPort;
    }

    public List<Integer> getPartitions() {
        return partitions;
    }

    public void setPartitions(List<Integer> partitions) {
        this.partitions = partitions;
    }

}
