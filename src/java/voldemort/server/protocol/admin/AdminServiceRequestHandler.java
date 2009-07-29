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

package voldemort.server.protocol.admin;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.serialization.VoldemortOpCode;
import voldemort.server.StoreRepository;
import voldemort.server.UnableUpdateMetadataException;
import voldemort.server.VoldemortMetadata;
import voldemort.server.protocol.RequestHandler;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.ClosableIterator;
import voldemort.utils.IoThrottler;
import voldemort.utils.Pair;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

/**
 * Responsible for interpreting and handling a Admin Request Stream
 * 
 * @author bbansal
 * 
 */
public class AdminServiceRequestHandler implements RequestHandler {

    private final Logger logger = Logger.getLogger(AdminServiceRequestHandler.class);

    private final StoreRepository storeRepository;
    private final VoldemortMetadata metadata;
    private final MetadataStore metadataStore;

    private final ErrorCodeMapper errorMapper;
    private final int streamMaxBytesReadPerSec;
    private final int streamMaxBytesWritesPerSec;

    public AdminServiceRequestHandler(ErrorCodeMapper errorMapper,
                                      StoreRepository storeRepository,
                                      VoldemortMetadata metadata,
                                      String metadataDir,
                                      int streamMaxBytesReadPerSec,
                                      int streamMaxBytesWritesPerSec) {
        this.storeRepository = storeRepository;
        this.metadata = metadata;
        this.errorMapper = errorMapper;
        this.metadataStore = MetadataStore.readFromDirectory(new File(metadataDir));
        this.streamMaxBytesReadPerSec = streamMaxBytesReadPerSec;
        this.streamMaxBytesWritesPerSec = streamMaxBytesWritesPerSec;
    }

    public void handleRequest(DataInputStream inputStream, DataOutputStream outputStream)
            throws IOException {
        byte opCode = inputStream.readByte();
        StorageEngine<ByteArray, byte[]> engine;
        switch(opCode) {
            case VoldemortOpCode.GET_PARTITION_AS_STREAM_OP_CODE:
                engine = readStorageEngine(inputStream, outputStream);
                if(engine != null)
                    handleGetPartitionsAsStream(engine, inputStream, outputStream);
                break;
            case VoldemortOpCode.PUT_ENTRIES_AS_STREAM_OP_CODE:
                engine = readStorageEngine(inputStream, outputStream);
                if(engine != null)
                    handleUpdateEntries(engine, inputStream, outputStream);
                break;
            case VoldemortOpCode.UPDATE_METADATA_OP_CODE:
                String keyString = inputStream.readUTF();
                handleUpdateMetadataRequest(keyString, inputStream, outputStream);
                break;

            case VoldemortOpCode.SERVER_STATE_CHANGE_OP_CODE:
                handleServerStateChangeRequest(inputStream, outputStream);
                break;
            case VoldemortOpCode.REDIRECT_GET_OP_CODE:
                engine = readStorageEngine(inputStream, outputStream);
                byte[] key = readKey(inputStream);
                handleRedirectGetRequest(engine, key, outputStream);
                break;
            default:
                throw new IOException("Unknown op code : " + opCode + " at Node:"
                                      + metadata.getIdentityNode().getId());
        }

        outputStream.flush();
    }

    public boolean isCompleteRequest(ByteBuffer buffer) {
        throw new UnsupportedOperationException();
    }

    private byte[] readKey(DataInputStream inputStream) throws IOException {
        int keySize = inputStream.readInt();
        byte[] key = new byte[keySize];
        ByteUtils.read(inputStream, key);

        return key;
    }

    private StorageEngine<ByteArray, byte[]> readStorageEngine(DataInputStream inputStream,
                                                               DataOutputStream outputStream)
            throws IOException {
        String storeName = inputStream.readUTF();
        StorageEngine<ByteArray, byte[]> engine = storeRepository.getStorageEngine(storeName);
        if(engine == null) {
            writeException(outputStream, new VoldemortException("No store named '" + storeName
                                                                + "'."));
        }

        return engine;
    }

    /**
     * provides a way to write a batch of entries to a storageEngine. expects
     * format as
     * <p>
     * <code>KeyLength(int32) bytes(keyLength) valueLength(int32)
     * bytes(valueLength)</code>
     * <p>
     * <strong> Reads entries unless see a keyLength value of -1</strong>.
     * <p>
     * Possible usecases
     * <ul>
     * <li>data grandfathering</li>
     * <li>cluster rebalancing</li>
     * <li>nodes replication</li>
     * </ul>
     * 
     * @param engine
     * @throws IOException
     */
    private void handleUpdateEntries(StorageEngine<ByteArray, byte[]> engine,
                                     DataInputStream inputStream,
                                     DataOutputStream outputStream) throws IOException {
        IoThrottler throttler = new IoThrottler(streamMaxBytesWritesPerSec);

        try {
            int keySize = inputStream.readInt();
            while(keySize != -1) {
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

                engine.put(new ByteArray(key), versionedValue);

                if(throttler != null) {
                    throttler.maybeThrottle(key.length + clock.sizeInBytes() + value.length);
                }

                keySize = inputStream.readInt(); // read next KeySize
            }
            // all puts are handled.
            outputStream.writeShort(0);
        } catch(VoldemortException e) {
            writeException(outputStream, e);
        }
    }

    /**
     * provides a way to read batch entries from a storageEngine. expects an
     * integer list of partitions requested. writes back to dataStream in format
     * <p>
     * <code>KeyLength(int32) bytes(keyLength) valueLength(int32)
     * bytes(valueLength)</code>
     * <p>
     * <strong>Stream end is indicated by writing a keyLength value of
     * -1</strong>.
     * <p>
     * Possible usecases
     * <ul>
     * <li>data grandfathering</li>
     * <li>cluster rebalancing</li>
     * <li>nodes replication</li>
     * </ul>
     * 
     * @param engine
     * @throws IOException
     */
    private void handleGetPartitionsAsStream(StorageEngine<ByteArray, byte[]> engine,
                                             DataInputStream inputStream,
                                             DataOutputStream outputStream) throws IOException {
        // read partition List
        int partitionSize = inputStream.readInt();
        int[] partitionList = new int[partitionSize];
        for(int i = 0; i < partitionSize; i++) {
            partitionList[i] = inputStream.readInt();
        }

        RoutingStrategy routingStrategy = new RoutingStrategyFactory(metadata.getCurrentCluster()).getRoutingStrategy(metadata.getStoreDef(engine.getName()));
        IoThrottler throttler = new IoThrottler(streamMaxBytesReadPerSec);
        try {
            /**
             * TODO HIGH: This way to iterate over all keys is not optimal
             * stores should be made routing aware to fix this problem
             */
            ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> iterator = engine.entries();

            while(iterator.hasNext()) {
                Pair<ByteArray, Versioned<byte[]>> entry = iterator.next();

                if(validPartition(entry.getFirst().get(), partitionList, routingStrategy)) {
                    outputStream.writeShort(0);

                    // write key
                    byte[] key = entry.getFirst().get();
                    outputStream.writeInt(key.length);
                    outputStream.write(key);

                    // write value
                    byte[] clock = ((VectorClock) entry.getSecond().getVersion()).toBytes();
                    byte[] value = entry.getSecond().getValue();
                    outputStream.writeInt(clock.length + value.length);
                    outputStream.write(clock);
                    outputStream.write(value);

                    if(throttler != null) {
                        throttler.maybeThrottle(key.length + clock.length + value.length);
                    }
                }
            }
            // close the iterator here
            iterator.close();
            // client reads exception before every key length
            outputStream.writeShort(0);
            // indicate that all keys are done
            outputStream.writeInt(-1);
            outputStream.flush();

        } catch(VoldemortException e) {
            writeException(outputStream, e);
        }
    }

    private void handleUpdateMetadataRequest(String keyString,
                                             DataInputStream inputStream,
                                             DataOutputStream outputStream) throws IOException {
        if(keyString.equals(MetadataStore.CLUSTER_KEY)
           || keyString.equals(MetadataStore.ROLLBACK_CLUSTER_KEY)) {
            handleUpdateClusterMetadataRequest(keyString, inputStream, outputStream);
        } else if(keyString.equals(MetadataStore.STORES_KEY)) {
            handleUpdateStoresMetadataRequest(keyString, inputStream, outputStream);
        } else {
            writeException(outputStream, new VoldemortException("Metadata Key passed " + keyString
                                                                + " is not handled yet ..."));
        }
    }

    private void handleUpdateClusterMetadataRequest(String cluster_key,
                                                    DataInputStream inputStream,
                                                    DataOutputStream outputStream)
            throws IOException {
        // get current ClusterInfo
        List<Versioned<byte[]>> clusterInfo = metadataStore.get(ByteUtils.getBytes(cluster_key,
                                                                                   "UTF-8"));
        if(clusterInfo.size() > 1) {
            throw new UnableUpdateMetadataException("Inconistent Cluster Metdata found on Server:"
                                                    + metadata.getIdentityNode().getId()
                                                    + " for Key:" + cluster_key);
        }
        logger.debug("Cluster metadata  update called " + cluster_key);
        // update version
        VectorClock updatedVersion = new VectorClock();
        if(clusterInfo.size() > 0) {
            updatedVersion = ((VectorClock) clusterInfo.get(0).getVersion());
        }

        updatedVersion.incrementVersion(metadata.getIdentityNode().getId(),
                                        System.currentTimeMillis());

        try {
            String clusterString = inputStream.readUTF();

            Cluster updatedCluster = new ClusterMapper().readCluster(new StringReader(clusterString));

            // update cluster details in metaDataStore
            metadataStore.put(new ByteArray(ByteUtils.getBytes(cluster_key, "UTF-8")),
                              new Versioned<byte[]>(ByteUtils.getBytes(new ClusterMapper().writeCluster(updatedCluster),
                                                                       "UTF-8"),
                                                    updatedVersion));
            // if successfull update state in Voldemort Metadata
            if(MetadataStore.CLUSTER_KEY.equalsIgnoreCase(cluster_key)) {
                metadata.setCurrentCluster(updatedCluster);
                metadata.reinitRoutingStrategies();
            } else if(MetadataStore.ROLLBACK_CLUSTER_KEY.equalsIgnoreCase(cluster_key)) {
                metadata.setRollbackCluster(updatedCluster);
            }
            outputStream.writeShort(0);
        } catch(VoldemortException e) {
            e.printStackTrace();
            writeException(outputStream, e);
            return;
        }
    }

    private void handleUpdateStoresMetadataRequest(String storeKey,
                                                   DataInputStream inputStream,
                                                   DataOutputStream outputStream)
            throws IOException {
        // get current Store Info
        List<Versioned<byte[]>> storesInfo = metadataStore.get(ByteUtils.getBytes(storeKey, "UTF-8"));
        if(storesInfo.size() > 1) {
            throw new UnableUpdateMetadataException("Inconistent Stores Metdata found on Server:"
                                                    + metadata.getIdentityNode().getId());
        }

        // update version
        VectorClock updatedVersion = ((VectorClock) storesInfo.get(0).getVersion());
        updatedVersion.incrementVersion(metadata.getIdentityNode().getId(),
                                        System.currentTimeMillis());

        try {
            String storesString = inputStream.readUTF();

            List<StoreDefinition> storeDefs = new StoreDefinitionsMapper().readStoreList(new StringReader(storesString));

            // update cluster details in metaDataStore
            metadataStore.put(new ByteArray(ByteUtils.getBytes(MetadataStore.STORES_KEY, "UTF-8")),
                              new Versioned<byte[]>(ByteUtils.getBytes(new StoreDefinitionsMapper().writeStoreList(storeDefs),
                                                                       "UTF-8"),
                                                    updatedVersion));
            metadata.setStoreDefs(storeDefs);
            // if successfull update state in Voldemort Metadata
            metadata.setStoreDefMap(storeDefs);
            metadata.reinitRoutingStrategies();
            outputStream.writeShort(0);
        } catch(VoldemortException e) {
            e.printStackTrace();
            writeException(outputStream, e);
            return;
        }
    }

    private void handleServerStateChangeRequest(DataInputStream inputStream,
                                                DataOutputStream outputStream) throws IOException {
        try {
            VoldemortMetadata.ServerState newState = VoldemortMetadata.ServerState.valueOf(inputStream.readUTF());
            metadata.setServerState(newState);

            outputStream.writeShort(0);
        } catch(VoldemortException e) {
            e.printStackTrace();
            writeException(outputStream, e);
            return;
        }

    }

    /**
     * By pass store level consistency checks needed to handle redirect gets
     * while rebalancing
     * 
     * @param engine
     * @param key
     * @throws IOException
     */
    private void handleRedirectGetRequest(StorageEngine<ByteArray, byte[]> engine,
                                          byte[] key,
                                          DataOutputStream outputStream) throws IOException {
        List<Versioned<byte[]>> results = null;
        try {
            results = engine.get(new ByteArray(key));
            outputStream.writeShort(0);
        } catch(VoldemortException e) {
            e.printStackTrace();
            writeException(outputStream, e);
            return;
        }
        outputStream.writeInt(results.size());
        for(Versioned<byte[]> v: results) {
            byte[] clock = ((VectorClock) v.getVersion()).toBytes();
            byte[] value = v.getValue();
            outputStream.writeInt(clock.length + value.length);
            outputStream.write(clock);
            outputStream.write(value);
        }
    }

    private void writeException(DataOutputStream stream, VoldemortException e) throws IOException {
        short code = errorMapper.getCode(e);
        stream.writeShort(code);
        stream.writeUTF(e.getMessage());
    }

    private boolean validPartition(byte[] key, int[] partitionList, RoutingStrategy routingStrategy) {
        List<Integer> keyPartitions = routingStrategy.getPartitionList(key);
        for(int p: partitionList) {
            if(keyPartitions.contains(p)) {
                return true;
            }
        }
        return false;
    }

}
