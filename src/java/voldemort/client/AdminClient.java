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

package voldemort.client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.serialization.VoldemortOpCode;
import voldemort.server.VoldemortMetadata;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.socket.SocketAndStreams;
import voldemort.store.socket.SocketDestination;
import voldemort.store.socket.SocketPool;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.Pair;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.AbstractIterator;

/**
 * The client implementation for Admin Client hides socket level details from
 * user
 * 
 * @author bbansal
 */
public class AdminClient {

    private static final Logger logger = Logger.getLogger(AdminClient.class);
    private final ErrorCodeMapper errorCodeMapper = new ErrorCodeMapper();

    private final Node currentNode;
    private final SocketPool pool;
    private final VoldemortMetadata metadata;

    public AdminClient(Node currentNode, VoldemortMetadata metadata, SocketPool socketPool) {
        this.currentNode = currentNode;
        this.metadata = metadata;
        this.pool = socketPool;
    }

    public void close() throws VoldemortException {
    // don't close the socket pool, it is shared
    }

    public Node getConnectedNode() {
        return currentNode;
    }

    public VoldemortMetadata getMetadata() {
        return metadata;
    }

    public void updateClusterMetadata(int nodeId, Cluster cluster, String cluster_key)
            throws VoldemortException {
        Node node = metadata.getCurrentCluster().getNodeById(nodeId);
        SocketDestination destination = new SocketDestination(node.getHost(), node.getAdminPort());
        SocketAndStreams sands = pool.checkout(destination);
        try {
            DataOutputStream outputStream = sands.getOutputStream();
            outputStream.writeByte(VoldemortOpCode.UPDATE_METADATA_OP_CODE);
            outputStream.writeUTF(cluster_key);
            String clusterString = new ClusterMapper().writeCluster(cluster);
            outputStream.writeUTF(clusterString);
            outputStream.flush();

            DataInputStream inputStream = sands.getInputStream();
            checkException(inputStream);
        } catch(IOException e) {
            close(sands.getSocket());
            throw new VoldemortException(e);
        } finally {
            pool.checkin(destination, sands);
        }
    }

    public void updateStoresMetadata(int nodeId, List<StoreDefinition> storesList)
            throws VoldemortException {
        Node node = metadata.getCurrentCluster().getNodeById(nodeId);

        SocketDestination destination = new SocketDestination(node.getHost(), node.getAdminPort());
        SocketAndStreams sands = pool.checkout(destination);
        try {
            DataOutputStream outputStream = sands.getOutputStream();
            outputStream.writeByte(VoldemortOpCode.UPDATE_METADATA_OP_CODE);
            outputStream.writeUTF(MetadataStore.STORES_KEY);
            String storeDefString = new StoreDefinitionsMapper().writeStoreList(storesList);
            outputStream.writeUTF(storeDefString);
            outputStream.flush();

            DataInputStream inputStream = sands.getInputStream();
            checkException(inputStream);
        } catch(IOException e) {
            close(sands.getSocket());
            throw new VoldemortException(e);
        } finally {
            pool.checkin(destination, sands);
        }
    }

    public Iterator<Pair<ByteArray, Versioned<byte[]>>> fetchPartitionEntries(int nodeId,
                                                                                String storeName,
                                                                                List<Integer> partitionList)
            throws VoldemortException {
        Node node = metadata.getCurrentCluster().getNodeById(nodeId);
        final SocketDestination destination = new SocketDestination(node.getHost(),
                                                                    node.getAdminPort());
        final SocketAndStreams sands = pool.checkout(destination);
        try {
            // get these partitions from the node for store
            DataOutputStream getOutputStream = sands.getOutputStream();

            // send request for get Partition List
            getOutputStream.writeByte(VoldemortOpCode.GET_PARTITION_AS_STREAM_OP_CODE);
            getOutputStream.writeUTF(storeName);
            getOutputStream.writeInt(partitionList.size());
            for(Integer p: partitionList) {
                getOutputStream.writeInt(p.intValue());
            }
            getOutputStream.flush();

        } catch(IOException e) {
            close(sands.getSocket());
            throw new VoldemortException(e);
        }

        // read values
        final DataInputStream inputStream = sands.getInputStream();

        return new AbstractIterator<Pair<ByteArray, Versioned<byte[]>>>() {

            @Override
            protected Pair<ByteArray, Versioned<byte[]>> computeNext() {
                try {
                    checkException(inputStream);

                    int keySize = inputStream.readInt();
                    if(keySize == -1) {
                        pool.checkin(destination, sands);
                        return endOfData();
                    } else {
                        byte[] key = new byte[keySize];
                        ByteUtils.read(inputStream, key);

                        int valueSize = inputStream.readInt();
                        byte[] value = new byte[valueSize];
                        ByteUtils.read(inputStream, value);

                        VectorClock clock = new VectorClock(value);
                        Versioned<byte[]> versionedValue = new Versioned<byte[]>(ByteUtils.copy(value,
                                                                                                clock.sizeInBytes(),
                                                                                                value.length),
                                                                                 clock);
                        return Pair.create(new ByteArray(key), versionedValue);
                    }

                } catch(IOException e) {
                    close(sands.getSocket());
                    pool.checkin(destination, sands);
                    throw new VoldemortException(e);
                }
            }
        };
    }

    public void updatePartitionEntries(int nodeId,
                                       String storeName,
                                       Iterator<Pair<ByteArray, Versioned<byte[]>>> entryIterator)
            throws VoldemortException, IOException {
        Node node = metadata.getCurrentCluster().getNodeById(nodeId);

        SocketDestination destination = new SocketDestination(node.getHost(), node.getAdminPort());
        SocketAndStreams sands = pool.checkout(destination);
        DataOutputStream outputStream = sands.getOutputStream();
        DataInputStream inputStream = sands.getInputStream();

        try {
            // send request for put partitions
            outputStream.writeByte(VoldemortOpCode.PUT_ENTRIES_AS_STREAM_OP_CODE);
            outputStream.writeUTF(storeName);
            outputStream.flush();

            while(entryIterator.hasNext()) {
                Pair<ByteArray, Versioned<byte[]>> entry = entryIterator.next();
                outputStream.writeInt(entry.getFirst().length());
                outputStream.write(entry.getFirst().get());

                Versioned<byte[]> value = entry.getSecond();
                VectorClock clock = (VectorClock) value.getVersion();
                outputStream.writeInt(value.getValue().length + clock.sizeInBytes());
                outputStream.write(clock.toBytes());
                outputStream.write(value.getValue());

                outputStream.flush();
            }
            outputStream.writeInt(-1);
            outputStream.flush();

            // read values

        } catch(IOException e) {
            close(sands.getSocket());
            throw new VoldemortException(e);
        } finally {
            // check for Exception after each entry
            checkException(inputStream);
            pool.checkin(destination, sands);
        }
    }

    public void changeStateAndRefresh(int nodeId, VoldemortMetadata.ServerState state) {
        Cluster currentCluster = metadata.getCurrentCluster();
        Node node = currentCluster.getNodeById(nodeId);
        SocketDestination destination = new SocketDestination(node.getHost(), node.getAdminPort());
        System.out.println("Trying to check out socket for destination:" + destination);
        SocketAndStreams sands = pool.checkout(destination);
        try {
            DataOutputStream outputStream = sands.getOutputStream();
            outputStream.writeByte(VoldemortOpCode.SERVER_STATE_CHANGE_OP_CODE);
            outputStream.writeUTF(state.toString());
            outputStream.flush();

            DataInputStream inputStream = sands.getInputStream();
            checkException(inputStream);
        } catch(IOException e) {
            close(sands.getSocket());
            throw new VoldemortException(e);
        } finally {
            pool.checkin(destination, sands);
        }
    }

    public List<Versioned<byte[]>> redirectGet(int redirectedNodeId, String storeName, ByteArray key) {
        Node redirectedNode = metadata.getCurrentCluster().getNodeById(redirectedNodeId);
        SocketDestination destination = new SocketDestination(redirectedNode.getHost(),
                                                              redirectedNode.getAdminPort());
        SocketAndStreams sands = pool.checkout(destination);
        try {
            DataOutputStream outputStream = sands.getOutputStream();
            outputStream.writeByte(VoldemortOpCode.REDIRECT_GET_OP_CODE);
            outputStream.writeUTF(storeName);
            outputStream.writeInt(key.length());
            outputStream.write(key.get());
            outputStream.flush();
            DataInputStream inputStream = sands.getInputStream();
            checkException(inputStream);
            int resultSize = inputStream.readInt();
            List<Versioned<byte[]>> results = new ArrayList<Versioned<byte[]>>(resultSize);
            for(int i = 0; i < resultSize; i++) {
                int valueSize = inputStream.readInt();
                byte[] bytes = new byte[valueSize];
                ByteUtils.read(inputStream, bytes);
                VectorClock clock = new VectorClock(bytes);
                results.add(new Versioned<byte[]>(ByteUtils.copy(bytes,
                                                                 clock.sizeInBytes(),
                                                                 bytes.length), clock));
            }
            return results;
        } catch(IOException e) {
            close(sands.getSocket());
            throw new VoldemortException(e);
        } finally {
            pool.checkin(destination, sands);
        }
    }

    public Cluster getTempCluster(Cluster currentCluster,
                                  Node fromNode,
                                  Node toNode,
                                  List<Integer> stealList) {
        ArrayList<Node> nodes = new ArrayList<Node>();
        for(Node node: currentCluster.getNodes()) {
            if(fromNode.getId() == node.getId()) {
                List<Integer> partitionList = new ArrayList<Integer>(node.getPartitionIds());
                partitionList.removeAll(stealList);
                nodes.add(new Node(node.getId(),
                                   node.getHost(),
                                   node.getHttpPort(),
                                   node.getSocketPort(),
                                   node.getAdminPort(),
                                   partitionList,
                                   node.getStatus()));
            } else if(toNode.getId() == node.getId()) {
                stealList.addAll(node.getPartitionIds());
                nodes.add(new Node(node.getId(),
                                   node.getHost(),
                                   node.getHttpPort(),
                                   node.getSocketPort(),
                                   node.getAdminPort(),
                                   stealList,
                                   node.getStatus()));
            } else {
                nodes.add(node);
            }
        }
        return new Cluster(currentCluster.getName(), nodes);
    }

    public void pipeGetAndPutStreams(int donorNodeId,
                                     int stealerNodeId,
                                     String storeName,
                                     List<Integer> stealList) throws IOException {
        updatePartitionEntries(stealerNodeId, storeName, fetchPartitionEntries(donorNodeId,
                                                                                 storeName,
                                                                                 stealList));
    }

    public List<Integer> getStealList(Cluster old, Cluster updated, int fromNode, int toNode) {
        ArrayList<Integer> stealList = new ArrayList<Integer>();
        List<Integer> oldPartitions = old.getNodeById(fromNode).getPartitionIds();
        List<Integer> updatedPartitions = updated.getNodeById(toNode).getPartitionIds();

        for(Integer p: updatedPartitions) {
            if(oldPartitions.contains(p)) {
                stealList.add(p);
            }
        }
        return stealList;
    }

    private void checkException(DataInputStream inputStream) throws IOException {
        short retCode = inputStream.readShort();
        if(retCode != 0) {
            String error = inputStream.readUTF();
            throw errorCodeMapper.getError(retCode, error);
        }
    }

    private void close(Socket socket) {
        try {
            socket.close();
        } catch(IOException e) {
            logger.warn("Failed to close socket");
        }
    }

}
