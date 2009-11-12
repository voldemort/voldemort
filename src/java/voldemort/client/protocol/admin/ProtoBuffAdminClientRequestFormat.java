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

package voldemort.client.protocol.admin;

import com.google.common.collect.AbstractIterator;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.apache.log4j.Logger;
import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormatType;
import voldemort.client.protocol.VoldemortFilter;

import voldemort.client.protocol.admin.filter.DefaultVoldemortFilter;
import voldemort.client.protocol.pb.ProtoUtils;
import voldemort.client.protocol.pb.VAdminProto;
import voldemort.client.protocol.pb.VProto;
import voldemort.cluster.Node;
import voldemort.server.protocol.admin.AsyncOperationStatus;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.StoreUtils;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.socket.SocketAndStreams;
import voldemort.store.socket.SocketDestination;
import voldemort.store.socket.SocketPool;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.NetworkClassLoader;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.*;

/**
 * Protocol buffers implementation for {@link voldemort.client.protocol.admin.AdminClient}
 * *
 * @author afeinber
 */
public class ProtoBuffAdminClientRequestFormat extends AdminClient {
    private final ErrorCodeMapper errorMapper;
    private final static Logger logger = Logger.getLogger(ProtoBuffAdminClientRequestFormat.class);
    private final SocketPool pool;
    private final NetworkClassLoader networkClassLoader;

    public ProtoBuffAdminClientRequestFormat(MetadataStore metadataStore, SocketPool pool) {
        super(metadataStore);
        this.errorMapper = new ErrorCodeMapper();
        this.pool = pool;
        this.networkClassLoader = new NetworkClassLoader(Thread.currentThread()
            .getContextClassLoader());
    }

    protected <T extends Message.Builder> T sendAndReceive(int nodeId, Message message, T builder) {
        Node node = this.getMetadata().getCluster().getNodeById(nodeId);
        SocketDestination destination = new SocketDestination(node.getHost(),
                node.getAdminPort(),
                RequestFormatType.ADMIN_PROTOCOL_BUFFERS);
        SocketAndStreams sands = pool.checkout(destination);

        try {
            DataOutputStream outputStream = sands.getOutputStream();
            DataInputStream inputStream = sands.getInputStream();
            ProtoUtils.writeMessage(outputStream, message);
            outputStream.flush();

            return ProtoUtils.readToBuilder(inputStream, builder);
        } catch (IOException e) {
            close(sands.getSocket());
            throw new VoldemortException(e);
        } finally {
            pool.checkin(destination, sands);
        }
    }

    /**
     * Updates Metadata at (remote) Node
     *
     * @param remoteNodeId Node id to update
     * @param key Key to update
     * @param value The metadata 
     * @throws VoldemortException
     */
    @Override
    protected void doUpdateRemoteMetadata(int remoteNodeId, ByteArray key, Versioned<byte[]> value) {
        VAdminProto.VoldemortAdminRequest request = VAdminProto.VoldemortAdminRequest.newBuilder()
                .setType(VAdminProto.AdminRequestType.UPDATE_METADATA)
                .setUpdateMetadata(VAdminProto.UpdateMetadataRequest.newBuilder()
                        .setKey(ByteString.copyFrom(key.get()))
                        .setVersioned(ProtoUtils.encodeVersioned(value))
                        .build())
                .build();
        VAdminProto.UpdateMetadataResponse.Builder response = sendAndReceive(remoteNodeId, request,
                VAdminProto.UpdateMetadataResponse.newBuilder());
        if (response.hasError())
            throwException(response.getError());
    }

    /**
     * Get Metadata from (remote) Node
     *
     * @param remoteNodeId
     * @param key
     * @throws VoldemortException
     */
    @Override
    protected Versioned<byte []> doGetRemoteMetadata(int remoteNodeId, ByteArray key) {
        VAdminProto.VoldemortAdminRequest request = VAdminProto.VoldemortAdminRequest.newBuilder()
                .setType(VAdminProto.AdminRequestType.GET_METADATA)
                .setGetMetadata(VAdminProto.GetMetadataRequest.newBuilder()
                        .setKey(ByteString.copyFrom(key.get())))
                .build();
        VAdminProto.GetMetadataResponse.Builder response = sendAndReceive(remoteNodeId, request,
                VAdminProto.GetMetadataResponse.newBuilder());

        if (response.hasError())
            throwException(response.getError());

        return ProtoUtils.decodeVersioned(response.getVersion());
    }

    /**
     * update Entries at (remote) node with all entries in iterator for passed
     * storeName
     *
     * @param nodeId
     * @param storeName
     * @param entryIterator
     * @param filter: <imp>Do not Update entries filtered out (returned
     *                       false) from the {@link VoldemortFilter} implementation</imp>
     * @throws VoldemortException
     * @throws IOException
     */
    public void updateEntries(int nodeId, String storeName,
                                         Iterator<Pair<ByteArray, Versioned<byte[]>>> entryIterator,
                                         VoldemortFilter filter) {
        Node node = this.getMetadata().getCluster().getNodeById(nodeId);
        SocketDestination destination = new SocketDestination(node.getHost(),
            node.getAdminPort(),
            RequestFormatType.ADMIN_PROTOCOL_BUFFERS);
        SocketAndStreams sands = pool.checkout(destination);
        DataOutputStream outputStream = sands.getOutputStream();
        DataInputStream inputStream = sands.getInputStream();
        boolean firstMessage=true;

        try {
            while (entryIterator.hasNext()) {
                Pair<ByteArray, Versioned<byte[]>> entry = entryIterator.next();
                VAdminProto.PartitionEntry partitionEntry =
                        VAdminProto.PartitionEntry.newBuilder()
                        .setKey(ProtoUtils.encodeBytes(entry.getFirst()))
                        .setVersioned(ProtoUtils.encodeVersioned(entry.getSecond()))
                        .build();
                VAdminProto.UpdatePartitionEntriesRequest.Builder updateRequest =
                        VAdminProto.UpdatePartitionEntriesRequest.newBuilder()
                        .setStore(storeName)
                        .setPartitionEntry(partitionEntry);

                if (firstMessage) {
                    if (filter != null) {
                        updateRequest.setFilter(encodeFilter(filter));
                    }
                    
                    ProtoUtils.writeMessage(outputStream,
                            VAdminProto.VoldemortAdminRequest.newBuilder()
                            .setType(VAdminProto.AdminRequestType.UPDATE_PARTITION_ENTRIES)
                            .setUpdatePartitionEntries(updateRequest).build());
                    outputStream.flush();
                    firstMessage = false;
                } else {
                    ProtoUtils.writeMessage(outputStream, updateRequest.build());
                }
            }
            ProtoUtils.writeEndOfStream(outputStream);
            outputStream.flush();
            VAdminProto.UpdatePartitionEntriesResponse.Builder updateResponse =
                    ProtoUtils.readToBuilder(inputStream,
                            VAdminProto.UpdatePartitionEntriesResponse.newBuilder());
            if (updateResponse.hasError()) {
                throwException(updateResponse.getError());
            }
        } catch (IOException e) {
            close(sands.getSocket());
            throw new VoldemortException(e);
        }  finally {
            pool.checkin(destination, sands);
        }
    }

    private void initiateFetchRequest(DataOutputStream outputStream, String storeName, List<Integer> partitionList,
                                      VoldemortFilter filter, boolean fetchValues) throws IOException {
        VAdminProto.FetchPartitionEntriesRequest.Builder fetchRequest =
                VAdminProto.FetchPartitionEntriesRequest.newBuilder()
                        .addAllPartitions(partitionList)
                        .setFetchValues(fetchValues)
                        .setStore(storeName);
        
        if (filter != null) {
            fetchRequest.setFilter(encodeFilter(filter));
        }

        VAdminProto.VoldemortAdminRequest request =
                VAdminProto.VoldemortAdminRequest.newBuilder()
                        .setType(VAdminProto.AdminRequestType.FETCH_PARTITION_ENTRIES)
                        .setFetchPartitionEntries(fetchRequest)
                .build();
        ProtoUtils.writeMessage(outputStream, request);
        outputStream.flush();

    }

    VAdminProto.FetchPartitionEntriesResponse responseFromStream(DataInputStream inputStream, int size) throws
            IOException {
        byte[] input = new byte[size];
        ByteUtils.read(inputStream, input);
        VAdminProto.FetchPartitionEntriesResponse.Builder response =
                VAdminProto.FetchPartitionEntriesResponse.newBuilder();
        response.mergeFrom(input);

        return response.build();
    }

    /**
     * streaming API to get all entries belonging to any of the partition in the
     * input List.
     *
     * @param nodeId
     * @param storeName
     * @param partitionList
     * @param filter: <imp>Do not fetch entries filtered out (returned
     *                       false) from the {@link VoldemortFilter} implementation</imp>
     * @return
     * @throws VoldemortException
     */
    public Iterator<Pair<ByteArray, Versioned<byte[]>>>
    fetchPartitionEntries(int nodeId, String storeName, List<Integer> partitionList,
                            VoldemortFilter filter) {
        Node node = this.getMetadata().getCluster().getNodeById(nodeId);
        final SocketDestination destination = new SocketDestination(node.getHost(),
                node.getAdminPort(),
                RequestFormatType.ADMIN_PROTOCOL_BUFFERS);
        final SocketAndStreams sands = pool.checkout(destination);
        DataOutputStream outputStream = sands.getOutputStream();
        final DataInputStream inputStream = sands.getInputStream();

        try {
            initiateFetchRequest(outputStream, storeName, partitionList, filter, true);
        } catch (IOException e) {
            close (sands.getSocket());
            pool.checkin(destination, sands);
            throw new VoldemortException(e);
        }

        return new AbstractIterator<Pair<ByteArray, Versioned<byte[]>>>() {
            @Override
            public Pair<ByteArray, Versioned<byte[]>> computeNext() {
                try {
                    int size = inputStream.readInt();
                    if (size <= 0) {
                        pool.checkin(destination, sands);
                        return endOfData();
                    }

                    VAdminProto.FetchPartitionEntriesResponse response =
                            responseFromStream(inputStream, size);

                    if (response.hasError()) {
                        pool.checkin(destination, sands);
                        throwException(response.getError());
                    }

                    VAdminProto.PartitionEntry partitionEntry = response.getPartitionEntry();

                    return Pair.create(ProtoUtils.decodeBytes(partitionEntry.getKey()),
                            ProtoUtils.decodeVersioned(partitionEntry.getVersioned()));
                } catch (IOException e) {
                    close(sands.getSocket());
                    pool.checkin(destination, sands);
                    throw new VoldemortException(e);
                }
            }
        };



    }

    /**
     * streaming API to get a list of all the keys that belong to any of the partitions
     * in the input list
     *
     * @param nodeId
     * @param storeName
     * @param partitionList
     * @param filter
     * @return
     */
    public Iterator<ByteArray> fetchPartitionKeys(int nodeId, String storeName, List<Integer> partitionList, VoldemortFilter filter) {
        Node node = this.getMetadata().getCluster().getNodeById(nodeId);
        final SocketDestination destination = new SocketDestination(node.getHost(),
            node.getAdminPort(),
            RequestFormatType.ADMIN_PROTOCOL_BUFFERS);
        final SocketAndStreams sands = pool.checkout(destination);
        DataOutputStream outputStream = sands.getOutputStream();
        final DataInputStream inputStream = sands.getInputStream();

        try {
            initiateFetchRequest(outputStream, storeName, partitionList, filter, false);
        } catch (IOException e) {
            close(sands.getSocket());
            pool.checkin(destination, sands);
            throw new VoldemortException(e);
        }

        return new AbstractIterator<ByteArray>() {
            @Override
            public ByteArray computeNext() {
                try {
                    int size = inputStream.readInt();
                    if (size <= 0) {
                        pool.checkin(destination, sands);
                        return endOfData();
                    }
                    
                    VAdminProto.FetchPartitionEntriesResponse response =
                            responseFromStream(inputStream, size);
                    
                    if (response.hasError()) {
                        pool.checkin(destination, sands);
                        throwException(response.getError());
                    }

                    return ProtoUtils.decodeBytes(response.getKey());
                } catch (IOException e) {
                    close(sands.getSocket());
                    pool.checkin(destination, sands);
                    throw new VoldemortException(e);
                }

            }
        };
    }

    /**
     * Pipe fetch from donorNode and update stealerNode in streaming mode.
     */
    @Override
    public void fetchAndUpdateStreams(int donorNodeId, int stealerNodeId, String storeName, List<Integer> stealList, VoldemortFilter filter) {
        VAdminProto.InitiateFetchAndUpdateRequest.Builder initiateFetchAndUpdateRequest =
                VAdminProto.InitiateFetchAndUpdateRequest.newBuilder()
                .setNodeId(donorNodeId)
                .addAllPartitions(stealList)
                .setStore(storeName);
        try {
            if (filter != null) {
                initiateFetchAndUpdateRequest.setFilter(encodeFilter(filter));
            }
        } catch (IOException e) {
            throw new VoldemortException(e);
        }

        VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                .setInitiateFetchAndUpdate(initiateFetchAndUpdateRequest)
                .setType(VAdminProto.AdminRequestType.INITIATE_FETCH_AND_UPDATE)
                .build();
        VAdminProto.AsyncOperationStatusResponse.Builder response = sendAndReceive(stealerNodeId, adminRequest,
                VAdminProto.AsyncOperationStatusResponse.newBuilder());

        if (response.hasError()) {
            throwException(response.getError());
        }

        /**
         * This uses exponential back off to wait for the request to finish on the stealer node
         * TODO: make waiting optional, add configurable delay parameters, use DelayQueue
         */
        long delay = 250;
        long maxDelay = 1000*60;
        int requestId = response.getRequestId();
        while (true) {
            AsyncOperationStatus status = getAsyncRequestStatus(stealerNodeId, requestId);
            if (status.isComplete())
                break;
            if (delay < maxDelay)
                delay *= 2;
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public AsyncOperationStatus getAsyncRequestStatus(int nodeId, int requestId) {
        VAdminProto.AsyncOperationStatusRequest asyncRequest = VAdminProto.AsyncOperationStatusRequest.newBuilder()
                .setRequestId(requestId)
                .build();
        VAdminProto.VoldemortAdminRequest adminRequest = VAdminProto.VoldemortAdminRequest.newBuilder()
                .setType(VAdminProto.AdminRequestType.ASYNC_OPERATION_STATUS)
                .setAsyncOperationStatus(asyncRequest)
                .build();
        VAdminProto.AsyncOperationStatusResponse.Builder response = sendAndReceive(nodeId, adminRequest,
                VAdminProto.AsyncOperationStatusResponse.newBuilder());

        if (response.hasError())
            throwException(response.getError());

        AsyncOperationStatus status = new AsyncOperationStatus(response.getRequestId(), response.getDescription());
        status.setStatus(response.getStatus());
        status.setComplete(response.getComplete());

        return status;
    }

    private VAdminProto.VoldemortFilter encodeFilter(VoldemortFilter filter) throws IOException {
        Class cl = filter.getClass();
        byte[] classBytes = networkClassLoader.dumpClass(cl);
        return VAdminProto.VoldemortFilter.newBuilder()
            .setName(cl.getName())
            .setData(ProtoUtils.encodeBytes(new ByteArray(classBytes)))
            .build();
    }

    /**
     * Delete all Entries at (remote) node for partitions in partitionList
     *
     * @param nodeId
     * @param storeName
     * @param partitionList
     * @param filter: <imp>Do not Delete entries filtered out (returned
     *                       false) from the {@link VoldemortFilter} implementation</imp>
     * @throws VoldemortException
     * @throws IOException
     */
    @Override
    public int deletePartitions(int nodeId, String storeName,
                                        List<Integer> partitionList,
                                        VoldemortFilter filter) {
        VAdminProto.DeletePartitionEntriesRequest.Builder deleteRequest =
                VAdminProto.DeletePartitionEntriesRequest.newBuilder()
                        .addAllPartitions(partitionList)
                        .setStore(storeName);
        
        try {
            if (filter != null) {
                deleteRequest.setFilter(encodeFilter(filter));
            }
        } catch (IOException e) {
            throw new VoldemortException(e);
        }

        VAdminProto.VoldemortAdminRequest request = VAdminProto.VoldemortAdminRequest
                .newBuilder()
                .setType(VAdminProto.AdminRequestType.DELETE_PARTITION_ENTRIES)
                .setDeletePartitionEntries(deleteRequest)
                .build();
        VAdminProto.DeletePartitionEntriesResponse.Builder response = sendAndReceive(nodeId,
                request,
                VAdminProto.DeletePartitionEntriesResponse.newBuilder());

        if (response.hasError())
            throwException(response.getError());

        return response.getCount();
    }

    public void throwException(VProto.Error error) {
            throw errorMapper.getError((short) error.getErrorCode(), error.getErrorMessage());
    }

    private void close(Socket socket) {
        try {
            socket.close();
        } catch(IOException e) {
            logger.warn("Failed to close socket");
        }
    }
}
