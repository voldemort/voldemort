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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormatType;
import voldemort.client.protocol.VoldemortFilter;
import voldemort.client.protocol.admin.filter.DefaultVoldemortFilter;
import voldemort.cluster.Node;
import voldemort.serialization.VoldemortOpCode;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.socket.SocketAndStreams;
import voldemort.store.socket.SocketDestination;
import voldemort.store.socket.SocketPool;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.NetworkClassLoader;
import voldemort.utils.Pair;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.common.collect.AbstractIterator;

/**
 * The Voldemort Native implementation for {@link AdminClientRequestFormat}
 * 
 * @author bbansal
 */
public class NativeAdminClientRequestFormat extends AdminClientRequestFormat {

    private static final Logger logger = Logger.getLogger(NativeAdminClientRequestFormat.class);
    private final ErrorCodeMapper errorCodeMapper = new ErrorCodeMapper();
    private final SocketPool pool;
    private final NetworkClassLoader networkClassLoader;

    public NativeAdminClientRequestFormat(MetadataStore metadata, SocketPool socketPool) {
        super(metadata);
        this.pool = socketPool;
        this.networkClassLoader = new NetworkClassLoader(Thread.currentThread()
                                                               .getContextClassLoader());
    }

    @Override
    public void doUpdateRemoteMetadata(int remoteNodeId, ByteArray key, Versioned<byte[]> value) {
        Node node = this.getMetadata().getCluster().getNodeById(remoteNodeId);

        SocketDestination destination = new SocketDestination(node.getHost(),
                                                              node.getSocketPort(),
                                                              RequestFormatType.ADMIN);
        SocketAndStreams sands = pool.checkout(destination);
        try {
            DataOutputStream outputStream = sands.getOutputStream();
            outputStream.writeByte(VoldemortOpCode.UPDATE_METADATA_OP_CODE);
            writeBytes(outputStream, key.get());
            outputStream.flush();

            // write versioned<byte[]> first clock bytes then version bytes
            VectorClock clock = (VectorClock) value.getVersion();
            outputStream.writeInt(clock.sizeInBytes() + value.getValue().length);
            outputStream.write(clock.toBytes());
            outputStream.write(value.getValue());
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

    @Override
    public Versioned<byte[]> doGetRemoteMetadata(int remoteNodeId, ByteArray key) {
        Node node = this.getMetadata().getCluster().getNodeById(remoteNodeId);

        SocketDestination destination = new SocketDestination(node.getHost(),
                                                              node.getSocketPort(),
                                                              RequestFormatType.ADMIN);
        SocketAndStreams sands = pool.checkout(destination);
        try {
            DataOutputStream outputStream = sands.getOutputStream();
            outputStream.writeByte(VoldemortOpCode.GET_METADATA_OP_CODE);
            writeBytes(outputStream, key.get());
            outputStream.flush();

            DataInputStream inputStream = sands.getInputStream();

            // read the length of array
            int size = inputStream.readInt();

            if(1 == size) {
                byte[] versionedData = readBytes(inputStream);
                VectorClock clock = new VectorClock(versionedData);
                byte[] data = ByteUtils.copy(versionedData,
                                             clock.sizeInBytes(),
                                             versionedData.length);

                Versioned<byte[]> versionedValue = new Versioned<byte[]>(data, clock);

                checkException(inputStream);
                return versionedValue;
            }

            throw new VoldemortException("Failed to read metadata "
                                         + ByteUtils.getString(key.get(), "UTF-8") + " from node:"
                                         + remoteNodeId);
        } catch(IOException e) {
            close(sands.getSocket());
            throw new VoldemortException(e);
        } finally {
            pool.checkin(destination, sands);
        }
    }

    @Override
    public List<Versioned<byte[]>> doRedirectGet(int proxyDestNodeId,
                                                 String storeName,
                                                 ByteArray key) {
        Node proxyDestNode = this.getMetadata().getCluster().getNodeById(proxyDestNodeId);
        SocketDestination destination = new SocketDestination(proxyDestNode.getHost(),
                                                              proxyDestNode.getSocketPort(),
                                                              RequestFormatType.ADMIN);
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

    @Override
    public Iterator<Pair<ByteArray, Versioned<byte[]>>> doFetchPartitionEntries(int nodeId,
                                                                                String storeName,
                                                                                List<Integer> partitionList,
                                                                                VoldemortFilter filter) {
        Node node = this.getMetadata().getCluster().getNodeById(nodeId);
        final SocketDestination destination = new SocketDestination(node.getHost(),
                                                                    node.getSocketPort(),
                                                                    RequestFormatType.ADMIN);
        final SocketAndStreams sands = pool.checkout(destination);
        try {
            // get these partitions from the node for store
            DataOutputStream outputStream = sands.getOutputStream();

            // send request for get Partition List
            outputStream.writeByte(VoldemortOpCode.GET_PARTITION_AS_STREAM_OP_CODE);
            outputStream.writeUTF(storeName);
            outputStream.writeInt(partitionList.size());
            for(Integer p: partitionList) {
                outputStream.writeInt(p.intValue());
            }

            // send filter class.
            writeFilterClassToStream(outputStream, filter);

            outputStream.flush();

        } catch(IOException e) {
            close(sands.getSocket());
            throw new VoldemortException(e);
        }

        // read values
        final DataInputStream inputStream = sands.getInputStream();

        return new AbstractIterator<Pair<ByteArray, Versioned<byte[]>>>() {

            @Override
            public Pair<ByteArray, Versioned<byte[]>> computeNext() {
                try {
                    int keySize = inputStream.readInt();
                    if(keySize == -1) {
                        checkException(inputStream);
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
                        checkException(inputStream);
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

    @Override
    public int doDeletePartitionEntries(int nodeId,
                                        String storeName,
                                        List<Integer> partitionList,
                                        VoldemortFilter filter) {
        Node node = this.getMetadata().getCluster().getNodeById(nodeId);
        final SocketDestination destination = new SocketDestination(node.getHost(),
                                                                    node.getSocketPort(),
                                                                    RequestFormatType.ADMIN);
        final SocketAndStreams sands = pool.checkout(destination);
        try {
            // get these partitions from the node for store
            DataOutputStream outputStream = sands.getOutputStream();

            // send request for get Partition List
            outputStream.writeByte(VoldemortOpCode.DELETE_PARTITIONS_OP_CODE);

            outputStream.writeUTF(storeName);
            // write partition List.
            outputStream.writeInt(partitionList.size());
            for(Integer p: partitionList) {
                outputStream.writeInt(p.intValue());
            }
            // send filter class.
            writeFilterClassToStream(outputStream, filter);

            outputStream.flush();

            // read values
            final DataInputStream inputStream = sands.getInputStream();
            int deleteSuccessCount = inputStream.readInt();

            // check exceptions
            checkException(inputStream);

            return deleteSuccessCount;

        } catch(IOException e) {
            close(sands.getSocket());
            throw new VoldemortException(e);
        }
    }

    @Override
    public void doUpdatePartitionEntries(int nodeId,
                                         String storeName,
                                         Iterator<Pair<ByteArray, Versioned<byte[]>>> entryIterator,
                                         VoldemortFilter filter) {
        Node node = this.getMetadata().getCluster().getNodeById(nodeId);
        filter = (null == filter) ? new DefaultVoldemortFilter() : filter;
        SocketDestination destination = new SocketDestination(node.getHost(),
                                                              node.getSocketPort(),
                                                              RequestFormatType.ADMIN);
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

                if(filter.filter(entry.getFirst(), entry.getSecond())) {
                    outputStream.writeInt(entry.getFirst().length());
                    outputStream.write(entry.getFirst().get());

                    Versioned<byte[]> value = entry.getSecond();
                    VectorClock clock = (VectorClock) value.getVersion();
                    outputStream.writeInt(value.getValue().length + clock.sizeInBytes());
                    outputStream.write(clock.toBytes());
                    outputStream.write(value.getValue());

                    outputStream.flush();

                    checkException(inputStream);
                }
            }
            outputStream.writeInt(-1);
            outputStream.flush();
        } catch(IOException e) {
            close(sands.getSocket());
            throw new VoldemortException(e);
        } finally {
            // check for Exception after each entry
            pool.checkin(destination, sands);
        }
    }

    private void writeBytes(DataOutputStream dos, byte[] data) throws IOException {
        dos.writeInt(data.length);
        dos.write(data);
    }

    private byte[] readBytes(DataInputStream din) throws IOException {
        int size = din.readInt();
        byte[] data = new byte[size];
        int readLen = din.read(data);
        if(size != readLen) {
            throw new VoldemortException("Unable to read Byte correctly expected size:" + size
                                         + " got size:" + readLen + " data:" + new String(data));
        }

        return data;
    }

    // send the class over wire
    private void writeFilterClassToStream(DataOutputStream outputStream, Object voldemortFilter)
            throws IOException {
        // pass Default Implementation if null.
        Class cl = (null == voldemortFilter) ? new DefaultVoldemortFilter().getClass()
                                            : voldemortFilter.getClass();
        byte[] classBytes = networkClassLoader.dumpClass(cl);
        outputStream.writeUTF(cl.getName());
        outputStream.writeInt(classBytes.length);
        outputStream.write(classBytes);
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
