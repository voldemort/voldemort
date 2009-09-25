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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import voldemort.VoldemortException;
import voldemort.routing.RoutingStrategy;
import voldemort.serialization.VoldemortOpCode;
import voldemort.server.StoreRepository;
import voldemort.server.protocol.RequestHandler;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.StorageEngine;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;

/**
 * An Abstract class to define AdminServerRequestInterface and provide helper
 * functions.
 * 
 * @author bbansal
 * 
 */
public abstract class AdminServiceRequestHandler implements RequestHandler {

    private final StoreRepository storeRepository;
    private final MetadataStore metadataStore;

    private final ErrorCodeMapper errorMapper;

    public AdminServiceRequestHandler(ErrorCodeMapper errorMapper,
                                      StoreRepository storeRepository,
                                      MetadataStore metadataStore) {
        this.storeRepository = storeRepository;
        this.errorMapper = errorMapper;
        this.metadataStore = metadataStore;
    }

    protected MetadataStore getMetadataStore() {
        return metadataStore;
    }

    public boolean isCompleteRequest(ByteBuffer buffer) {
        throw new VoldemortException("Non-blocking server not supported for AdminServiceRequestFormats !!");
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
                    handleUpdateEntriesAsStream(engine, inputStream, outputStream);
                break;
            case VoldemortOpCode.DELETE_PARTITIONS_OP_CODE:
                engine = readStorageEngine(inputStream, outputStream);
                if(engine != null)
                    handleDeletePartitions(engine, inputStream, outputStream);
                break;
            case VoldemortOpCode.UPDATE_METADATA_OP_CODE:
                byte[] key = readKey(inputStream);
                handleUpdateMetadataRequest(new ByteArray(key), inputStream, outputStream);
                break;
            case VoldemortOpCode.GET_METADATA_OP_CODE:
                key = readKey(inputStream);
                handleGetMetadataRequest(new ByteArray(key), inputStream, outputStream);
                break;

            case VoldemortOpCode.REDIRECT_GET_OP_CODE:
                engine = readStorageEngine(inputStream, outputStream);
                key = readKey(inputStream);
                handleRedirectGetRequest(engine, key, outputStream);
                break;
            default:
                throw new IOException("Unknown op code : " + opCode + " at Node:"
                                      + metadataStore.getNodeId());
        }

        outputStream.flush();
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
    protected abstract void handleUpdateEntriesAsStream(StorageEngine<ByteArray, byte[]> engine,
                                                        DataInputStream inputStream,
                                                        DataOutputStream outputStream)
            throws IOException;

    /**
     * provides a way to read batch entries from a storageEngine. expects an
     * integer list of partitions requested. writes back to dataStream in format
     * <p>
     * <code>KeyLength(int32) bytes(keyLength) valueLength(int32)
     * bytes(valueLength)</code>
     * <p>
     * <strong>Stream end is indicated by writing a keyLength value of -1</strong>.
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
    protected abstract void handleGetPartitionsAsStream(StorageEngine<ByteArray, byte[]> engine,
                                                        DataInputStream inputStream,
                                                        DataOutputStream outputStream)
            throws IOException;

    /**
     * Provides a way to delete entire partitions from a Node.
     * 
     * @param engine
     * @param inputStream
     * @param outputStream
     * @throws IOException
     */
    protected abstract void handleDeletePartitions(StorageEngine<ByteArray, byte[]> engine,
                                                   DataInputStream inputStream,
                                                   DataOutputStream outputStream)
            throws IOException;

    /**
     * handle update() requests for metadata keys.<br>
     * Metadata operations needs finer control over metadata changes and need
     * point to point operations as compared to general purpose get()/put()
     * operation provided by MetadataStore Store API.
     * 
     * @param keyString
     * @param inputStream
     * @param outputStream
     * @throws IOException
     */
    protected abstract void handleUpdateMetadataRequest(ByteArray key,
                                                        DataInputStream inputStream,
                                                        DataOutputStream outputStream)
            throws IOException;

    protected abstract void handleGetMetadataRequest(ByteArray keyString,
                                                     DataInputStream inputStream,
                                                     DataOutputStream outputStream)
            throws IOException;

    /**
     * By pass store level consistency checks needed to handle redirect gets
     * while rebalancing
     * 
     * @param engine
     * @param key
     * @throws IOException
     */
    protected abstract void handleRedirectGetRequest(StorageEngine<ByteArray, byte[]> engine,
                                                     byte[] key,
                                                     DataOutputStream outputStream)
            throws IOException;

    protected void writeException(DataOutputStream stream, VoldemortException e) throws IOException {
        short code = errorMapper.getCode(e);
        stream.writeShort(code);
        stream.writeUTF(e.getMessage());
    }

    protected boolean validPartition(byte[] key,
                                     int[] partitionList,
                                     RoutingStrategy routingStrategy) {
        List<Integer> keyPartitions = routingStrategy.getPartitionList(key);
        for(int p: partitionList) {
            if(keyPartitions.contains(p)) {
                return true;
            }
        }
        return false;
    }

}
