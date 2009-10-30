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
import voldemort.client.protocol.VoldemortFilter;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.server.StoreRepository;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.StorageEngine;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.ClosableIterator;
import voldemort.utils.EventThrottler;
import voldemort.utils.NetworkClassLoader;
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
    private final NetworkClassLoader networkClassLoader;

    public NativeAdminServiceRequestHandler(ErrorCodeMapper errorMapper,
                                            StoreRepository storeRepository,
                                            MetadataStore metadataStore,
                                            int streamMaxBytesReadPerSec,
                                            int streamMaxBytesWritesPerSec) {
        super(errorMapper, storeRepository, metadataStore);

        this.streamMaxBytesReadPerSec = streamMaxBytesReadPerSec;
        this.streamMaxBytesWritesPerSec = streamMaxBytesWritesPerSec;
        this.networkClassLoader = new NetworkClassLoader(Thread.currentThread()
                                                               .getContextClassLoader());
    }

    @Override
    protected void handleUpdateEntriesAsStream(StorageEngine<ByteArray, byte[]> engine,
                                               DataInputStream inputStream,
                                               DataOutputStream outputStream) throws IOException {
        EventThrottler throttler = new EventThrottler(streamMaxBytesWritesPerSec);

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
        // read filter Class and load it up.
        VoldemortFilter filter = readFilterClassFromStream(inputStream);

        RoutingStrategy routingStrategy = getMetadataStore().getRoutingStrategy(engine.getName());
        EventThrottler throttler = new EventThrottler(streamMaxBytesReadPerSec);

        ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> iterator = null;
        try {
            /**
             * TODO HIGH: This way to iterate over all keys is not optimal
             * stores should be made routing aware to fix this problem
             */
            iterator = engine.entries();

            while(iterator.hasNext()) {
                Pair<ByteArray, Versioned<byte[]>> entry = iterator.next();

                if(validPartition(entry.getFirst().get(), partitionList, routingStrategy)
                   && filter.filter(entry.getFirst(), entry.getSecond())) {

                    // write key
                    byte[] key = entry.getFirst().get();
                    outputStream.writeInt(key.length);
                    outputStream.flush();

                    // write key and value
                    outputStream.write(key);
                    byte[] clock = ((VectorClock) entry.getSecond().getVersion()).toBytes();
                    byte[] value = entry.getSecond().getValue();
                    outputStream.writeInt(clock.length + value.length);
                    outputStream.write(clock);
                    outputStream.write(value);

                    outputStream.writeShort(0);
                    outputStream.flush();

                    if(throttler != null) {
                        throttler.maybeThrottle(key.length + clock.length + value.length + 1);
                    }
                }
            }
            // indicate that all keys are done
            outputStream.writeInt(-1);
            outputStream.writeShort(0);

            outputStream.flush();
        } catch(VoldemortException e) {
            // indicate that all keys are done
            outputStream.writeInt(-1);
            writeException(outputStream, e);
        } finally {
            if(null != iterator)
                iterator.close();
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
        // read filter Class and load it up.
        VoldemortFilter filter = readFilterClassFromStream(inputStream);

        RoutingStrategy routingStrategy = getMetadataStore().getRoutingStrategy(engine.getName());

        EventThrottler throttler = new EventThrottler(streamMaxBytesReadPerSec);
        try {
            ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> iterator = engine.entries();

            int deleteSuccess = 0;
            while(iterator.hasNext()) {
                Pair<ByteArray, Versioned<byte[]>> entry = iterator.next();

                if(validPartition(entry.getFirst().get(), partitionList, routingStrategy)
                   && filter.filter(entry.getFirst(), entry.getSecond())) {
                    // delete the key/value tuple.
                    if(engine.delete(entry.getFirst(), entry.getSecond().getVersion())) {
                        deleteSuccess++;
                    }

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
        try {
            String keyString = ByteUtils.getString(key.get(), "UTF-8");
            if(MetadataStore.METADATA_KEYS.contains(keyString)) {
                // read the update versioned value
                int valueSize = inputStream.readInt();
                byte[] value = new byte[valueSize];
                ByteUtils.read(inputStream, value);

                VectorClock clock = new VectorClock(value);
                Versioned<byte[]> versionedValue = new Versioned<byte[]>(ByteUtils.copy(value,
                                                                                        clock.sizeInBytes(),
                                                                                        value.length),
                                                                         clock);

                getMetadataStore().put(new ByteArray(ByteUtils.getBytes(keyString, "UTF-8")),
                                       versionedValue);

                // indicate that we are done
                outputStream.writeShort(0);
                outputStream.flush();
            } else {
                throw new VoldemortException("Metadata Key passed " + keyString
                                             + " is not handled yet ...");
            }
        } catch(VoldemortException e) {
            writeException(outputStream, e);
        }
    }

    @Override
    protected void handleGetMetadataRequest(ByteArray key,
                                            DataInputStream inputStream,
                                            DataOutputStream outputStream) throws IOException {
        try {
            String keyString = ByteUtils.getString(key.get(), "UTF-8");

            if(MetadataStore.METADATA_KEYS.contains(keyString)) {
                List<Versioned<byte[]>> versionedValueList = getMetadataStore().get(new ByteArray(ByteUtils.getBytes(keyString,
                                                                                                                     "UTF-8")));
                // metadata should return only latest metadata if available
                int size = (versionedValueList.size() > 0) ? 1 : 0;
                outputStream.writeInt(size);
                outputStream.flush();

                // write value
                if(size > 0) {
                    Versioned<byte[]> versionedValue = versionedValueList.get(0);
                    byte[] clock = ((VectorClock) versionedValue.getVersion()).toBytes();
                    byte[] value = versionedValue.getValue();
                    outputStream.writeInt(clock.length + value.length);
                    outputStream.write(clock);
                    outputStream.write(value);
                    outputStream.flush();
                }

                // indicate that we are done
                outputStream.writeShort(0);
                outputStream.flush();
            } else {
                throw new VoldemortException("Metadata Key passed " + keyString
                                             + " is not handled yet ...");
            }

        } catch(VoldemortException e) {
            writeException(outputStream, e);
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

    private VoldemortFilter readFilterClassFromStream(DataInputStream inputStream) {
        try {
            String className = inputStream.readUTF();
            int size = inputStream.readInt();
            byte[] classBytes = new byte[size];
            int read = inputStream.read(classBytes);
            if(size != read)
                throw new VoldemortException("Failed to read classBytes properly expected:" + size
                                             + " read:" + read);
            Class<?> cl = networkClassLoader.loadClass(className, classBytes, 0, classBytes.length);

            return (VoldemortFilter) cl.newInstance();
        } catch(Exception e) {
            throw new VoldemortException("Failed to read and instantiate Filter class", e);
        }
    }
}
