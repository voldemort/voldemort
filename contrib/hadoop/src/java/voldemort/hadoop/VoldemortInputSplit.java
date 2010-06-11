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

package voldemort.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import voldemort.cluster.Node;

public class VoldemortInputSplit extends InputSplit implements Writable {

    private String storeName;
    private String hostName;
    private Integer nodeId;
    private Integer adminPort;

    public VoldemortInputSplit(String storeName, Node node) {
        this.storeName = storeName;
        this.hostName = node.getHost();
        this.nodeId = node.getId();
        this.adminPort = node.getAdminPort();
    }

    /**
     * Is used to order the splits so that the largest get processed first, in
     * an attempt to minimize the job runtime...Voldemort doesn't care!
     */
    @Override
    public long getLength() throws IOException, InterruptedException {
        return 0;
    }

    public String getStoreName() {
        return this.storeName;
    }

    public String getHostName() {
        return this.hostName;
    }

    public Integer getNodeId() {
        return this.nodeId;
    }

    public Integer getAdminPort() {
        return this.adminPort;
    }

    /**
     * Returns the location of the split. Since the current scheme is only one
     * mapper per node, this is an array of one node only.
     */
    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[] { this.hostName };
    }

    public void readFields(DataInput inputStream) throws IOException {
        this.storeName = inputStream.readUTF();
        this.hostName = inputStream.readUTF();
        this.nodeId = inputStream.readInt();
        this.adminPort = inputStream.readInt();
    }

    public void write(DataOutput outputStream) throws IOException {
        outputStream.writeUTF(storeName);
        outputStream.writeUTF(hostName);
        outputStream.writeInt(nodeId);
        outputStream.writeInt(adminPort);
    }

    protected VoldemortInputSplit() {}

    public static VoldemortInputSplit read(DataInput in) throws IOException {
        VoldemortInputSplit split = new VoldemortInputSplit();
        split.readFields(in);
        return split;
    }
}
