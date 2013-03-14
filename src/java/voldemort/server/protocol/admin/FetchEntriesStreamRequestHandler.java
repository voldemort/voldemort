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
import java.util.List;

import voldemort.client.protocol.pb.ProtoUtils;
import voldemort.client.protocol.pb.VAdminProto;
import voldemort.client.protocol.pb.VAdminProto.FetchPartitionEntriesRequest;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.stats.StreamingStats.Operation;
import voldemort.utils.ByteArray;
import voldemort.utils.NetworkClassLoader;
import voldemort.utils.StoreInstance;
import voldemort.versioning.Versioned;

import com.google.protobuf.Message;

/**
 * Fetches entries by scanning entire storage engine in storage-order.
 * <p>
 * For performance reason use storageEngine.keys() iterator to filter out
 * unwanted keys and then call storageEngine.get() for valid keys.
 * <p>
 */
public class FetchEntriesStreamRequestHandler extends FetchItemsStreamRequestHandler {

    public FetchEntriesStreamRequestHandler(FetchPartitionEntriesRequest request,
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
              Operation.FETCH_ENTRIES);
        logger.info("Starting fetch entries for store '" + storageEngine.getName()
                    + "' with replica to partition mapping " + replicaToPartitionList);
    }

    @Override
    public StreamRequestHandlerState handleRequest(DataInputStream inputStream,
                                                   DataOutputStream outputStream)
            throws IOException {
        if(!keyIterator.hasNext()) {
            return StreamRequestHandlerState.COMPLETE;
        }

        // NOTE: Storage time is accounted for somewhat incorrectly because
        // .hasNext() is invoked at end of method for the common case.

        // Since key reading (keyIterator.next()) is done separately from entry
        // fetching (storageEngine.get()), must be careful about when to invoke
        // reportStorageOpTime and when to invoke maybeThrottle().
        long startNs = System.nanoTime();
        ByteArray key = keyIterator.next();

        // Cannot invoke 'throttler.maybeThrottle(key.length());' here since
        // that would affect timing measurements of storage operations.

        boolean entryAccepted = false;
        if(!fetchOrphaned) {
            if(keyIsNeeded(key.get())) {
                entryAccepted = true;
            }
        } else {
            if(!StoreInstance.checkKeyBelongsToNode(key.get(), nodeId, initialCluster, storeDef)) {
                entryAccepted = true;
            }
        }

        if(entryAccepted) {
            List<Versioned<byte[]>> values = storageEngine.get(key, null);
            reportStorageOpTime(startNs);
            throttler.maybeThrottle(key.length());
            for(Versioned<byte[]> value: values) {

                if(filter.accept(key, value)) {
                    keyFetched(key.get());

                    VAdminProto.FetchPartitionEntriesResponse.Builder response = VAdminProto.FetchPartitionEntriesResponse.newBuilder();
                    VAdminProto.PartitionEntry partitionEntry = VAdminProto.PartitionEntry.newBuilder()
                                                                                          .setKey(ProtoUtils.encodeBytes(key))
                                                                                          .setVersioned(ProtoUtils.encodeVersioned(value))
                                                                                          .build();
                    response.setPartitionEntry(partitionEntry);
                    Message message = response.build();

                    sendMessage(outputStream, message);

                    throttler.maybeThrottle(AdminServiceRequestHandler.valueSize(value));
                }
            }
        } else {
            reportStorageOpTime(startNs);
            throttler.maybeThrottle(key.length());
        }

        // log progress
        scanned++;
        if(0 == scanned % STAT_RECORDS_INTERVAL) {
            progressInfoMessage("Fetch entries (progress)");
        }

        if(keyIterator.hasNext() && !fetchedEnough()) {
            return StreamRequestHandlerState.WRITING;
        } else {
            logger.info("Finished fetch entries for store '" + storageEngine.getName()
                        + "' with replica to partition mapping " + replicaToPartitionList);
            progressInfoMessage("Fetch entries (end of scan)");

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
