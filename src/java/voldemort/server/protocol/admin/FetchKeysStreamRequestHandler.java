/*
 * Copyright 2013 LinkedIn, Inc
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

import voldemort.client.protocol.pb.ProtoUtils;
import voldemort.client.protocol.pb.VAdminProto;
import voldemort.client.protocol.pb.VAdminProto.FetchPartitionEntriesRequest;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.stats.StreamingStats.Operation;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.NetworkClassLoader;
import voldemort.utils.StoreInstance;

import com.google.protobuf.Message;

public class FetchKeysStreamRequestHandler extends FetchStreamRequestHandler {

    protected final ClosableIterator<ByteArray> keyIterator;

    public FetchKeysStreamRequestHandler(FetchPartitionEntriesRequest request,
                                         MetadataStore metadataStore,
                                         ErrorCodeMapper errorCodeMapper,
                                         VoldemortConfig voldemortConfig,
                                         StoreRepository storeRepository,
                                         NetworkClassLoader networkClassLoader) {
        super(request,
              metadataStore,
              errorCodeMapper,
              voldemortConfig,
              storeRepository,
              networkClassLoader,
              Operation.FETCH_KEYS);
        this.keyIterator = storageEngine.keys();
        logger.info("Starting fetch keys for store '" + storageEngine.getName()
                    + "' with replica to partition mapping " + replicaToPartitionList);
    }

    @Override
    public StreamRequestHandlerState handleRequest(DataInputStream inputStream,
                                                   DataOutputStream outputStream)
            throws IOException {
        if(!keyIterator.hasNext())
            return StreamRequestHandlerState.COMPLETE;

        long startNs = System.nanoTime();
        ByteArray key = keyIterator.next();
        if(streamStats != null) {
            streamStats.reportStorageTime(operation, System.nanoTime() - startNs);
            streamStats.reportStreamingScan(operation);
        }

        throttler.maybeThrottle(key.length());
        boolean keyAccepted = false;
        if(!fetchOrphaned) {
            if(StoreInstance.checkKeyBelongsToPartition(nodeId,
                                                        key.get(),
                                                        replicaToPartitionList,
                                                        initialCluster,
                                                        storeDef) && filter.accept(key, null)) {
                keyAccepted = true;
            }

        } else {
            if(!StoreInstance.checkKeyBelongsToNode(key.get(), nodeId, initialCluster, storeDef)) {
                keyAccepted = true;
            }
        }
        if(keyAccepted) {
            VAdminProto.FetchPartitionEntriesResponse.Builder response = VAdminProto.FetchPartitionEntriesResponse.newBuilder();
            response.setKey(ProtoUtils.encodeBytes(key));

            fetched++;
            if(streamStats != null)
                streamStats.reportStreamingFetch(operation);
            Message message = response.build();

            startNs = System.nanoTime();
            ProtoUtils.writeMessage(outputStream, message);
            if(streamStats != null) {
                // TODO: The accounting for streaming reads should also
                // move along with the next() call since we are indeed
                // fetching from disk.. ---VChandar

                streamStats.reportNetworkTime(operation, System.nanoTime() - startNs);
            }
        }

        // log progress
        counter++;

        if(0 == counter % STAT_RECORDS_INTERVAL) {
            long totalTime = (System.currentTimeMillis() - startTime) / 1000;

            logger.info("Fetch keys scanned " + counter + " keys, fetched " + fetched
                        + " keys for store '" + storageEngine.getName()
                        + "' replicaToPartitionList:" + replicaToPartitionList + " in " + totalTime
                        + " s");
        }

        // TODO: make usage clearer. Rename maxRecords to recordsPerPartition.
        // And, make recordsPerPartition <=0 mean 'get them all'.

        // TODO: Remove skipRecords from message and from code.

        // TODO: Make sure the distinction between FetchKeysStream and
        // FetchPartitionKeysStream is clear.

        // TODO: Add logic to FetchKeys and FetchEntries to count keys per
        // partition correctly.
        if(keyIterator.hasNext() && (counter < recordsPerPartition))
            return StreamRequestHandlerState.WRITING;
        else {
            return StreamRequestHandlerState.COMPLETE;
        }
    }

    @Override
    public final void close(DataOutputStream outputStream) throws IOException {
        if(null != keyIterator)
            keyIterator.close();
        super.close(outputStream);
    }
}
