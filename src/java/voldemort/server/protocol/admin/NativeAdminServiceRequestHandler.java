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
import java.util.List;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.server.StoreRepository;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.StorageEngine;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.ClosableIterator;
import voldemort.utils.IoThrottler;
import voldemort.utils.Pair;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

/**
 * The voldemort Native protocol implementation for
 * {@link AdminServiceRequestHandler}
 * 
 * @author bbansal
 * 
 */
public class NativeAdminServiceRequestHandler extends AdminServiceRequestHandler {

    private final Logger logger = Logger.getLogger(NativeAdminServiceRequestHandler.class);

    private final int streamMaxBytesReadPerSec;
    private final int streamMaxBytesWritesPerSec;
    private final RoutingStrategyFactory routingFactory = new RoutingStrategyFactory();

    public NativeAdminServiceRequestHandler(ErrorCodeMapper errorMapper,
                                            StoreRepository storeRepository,
                                            MetadataStore metadataStore,
                                            int streamMaxBytesReadPerSec,
                                            int streamMaxBytesWritesPerSec) {
        super(errorMapper, storeRepository, metadataStore);

        this.streamMaxBytesReadPerSec = streamMaxBytesReadPerSec;
        this.streamMaxBytesWritesPerSec = streamMaxBytesWritesPerSec;
    }

    @Override
    protected void handleUpdateEntriesAsStream(StorageEngine<ByteArray, byte[]> engine,
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

                outputStream.writeShort(0);
                outputStream.flush();

                keySize = inputStream.readInt(); // read next KeySize
            }
            // all puts are handled.
            outputStream.writeShort(0);
        } catch(VoldemortException e) {
            writeException(outputStream, e);
        }
    }

    @Override
    protected void handleGetPartitionsAsStream(StorageEngine<ByteArray, byte[]> engine,
                                               DataInputStream inputStream,
                                               DataOutputStream outputStream) throws IOException {
        // read partition List
        int partitionSize = inputStream.readInt();
        int[] partitionList = new int[partitionSize];
        for(int i = 0; i < partitionSize; i++) {
            partitionList[i] = inputStream.readInt();
        }

        RoutingStrategy routingStrategy = getMetadataStore().getRoutingStrategy(engine.getName());
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

    @Override
    protected void handleDeletePartitions(StorageEngine<ByteArray, byte[]> engine,
                                          DataInputStream inputStream,
                                          DataOutputStream outputStream) throws IOException {
        // read partition List
        int partitionSize = inputStream.readInt();
        int[] partitionList = new int[partitionSize];
        for(int i = 0; i < partitionSize; i++) {
            partitionList[i] = inputStream.readInt();
        }

        RoutingStrategy routingStrategy = getMetadataStore().getRoutingStrategy(engine.getName());

        IoThrottler throttler = new IoThrottler(streamMaxBytesReadPerSec);
        try {
            ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> iterator = engine.entries();

            int deleteSuccess = 0;
            while(iterator.hasNext()) {
                Pair<ByteArray, Versioned<byte[]>> entry = iterator.next();

                if(validPartition(entry.getFirst().get(), partitionList, routingStrategy)) {
                    // delete the key/value tuple.
                    if(engine.delete(entry.getFirst(), entry.getSecond().getVersion()))
                        deleteSuccess++;

                    // throttle for disk IO sake
                    if(throttler != null) {
                        throttler.maybeThrottle(entry.getFirst().get().length
                                                + entry.getSecond().getValue().length
                                                + ((VectorClock) entry.getSecond().getVersion()).sizeInBytes());
                    }
                }
            }
            // close the iterator here
            iterator.close();

            // send deleteSuccess Count
            outputStream.writeInt(deleteSuccess);

            // send no Exception value
            outputStream.writeShort(0);
            outputStream.flush();

        } catch(VoldemortException e) {
            writeException(outputStream, e);
        }
    }

    @Override
    protected void handleUpdateMetadataRequest(ByteArray key,
                                               DataInputStream inputStream,
                                               DataOutputStream outputStream) throws IOException {
        String keyString = ByteUtils.getString(key.get(), "UTF-8");
        if(MetadataStore.METADATA_KEYS.contains(keyString)) {
            // read the update versioned value
            int valueSize = inputStream.readInt();
            byte[] value = new byte[valueSize];
            ByteUtils.read(inputStream, value);

            VectorClock clock = new VectorClock(value);
            String valueString = ByteUtils.getString(ByteUtils.copy(value,
                                                                    clock.sizeInBytes(),
                                                                    value.length), "UTF-8");
            Versioned<byte[]> versionedValue = new Versioned<byte[]>(ByteUtils.getBytes(valueString,
                                                                                        "UTF-8"),
                                                                     clock);

            getMetadataStore().put(new ByteArray(ByteUtils.getBytes(keyString, "UTF-8")),
                                   versionedValue);

            // indicate that we are do"ne
            outputStream.writeInt(-1);
            outputStream.flush();
        } else {
            writeException(outputStream, new VoldemortException("Metadata Key passed " + keyString
                                                                + " is not handled yet ..."));
        }
    }

    @Override
    protected void handleGetMetadataRequest(ByteArray key,
                                            DataInputStream inputStream,
                                            DataOutputStream outputStream) throws IOException {
        String keyString = ByteUtils.getString(key.get(), "UTF-8");
        if(MetadataStore.METADATA_KEYS.contains(keyString)) {
            List<Versioned<byte[]>> versionedValueList = getMetadataStore().get(new ByteArray(ByteUtils.getBytes(keyString,
                                                                                                                 "UTF-8")));

            // metadata should return only latest metadata if available
            int size = (versionedValueList.size() > 0) ? 1 : 0;
            outputStream.writeInt(size);

            // write value
            Versioned<byte[]> versionedValue = versionedValueList.get(0);
            byte[] clock = ((VectorClock) versionedValue.getVersion()).toBytes();
            byte[] value = versionedValue.getValue();
            outputStream.writeInt(clock.length + value.length);
            outputStream.write(clock);
            outputStream.write(value);

            // indicate that we are done
            outputStream.writeInt(0);
            outputStream.flush();
        } else {
            writeException(outputStream, new VoldemortException("Metadata Key passed " + keyString
                                                                + " is not handled yet ..."));
        }
    }

    @Override
    protected void handleRedirectGetRequest(StorageEngine<ByteArray, byte[]> engine,
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
}
