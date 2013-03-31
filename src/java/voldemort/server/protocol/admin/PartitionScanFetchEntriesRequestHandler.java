/*
 * Copyright 2008-2013 LinkedIn, Inc
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
import voldemort.utils.Pair;
import voldemort.utils.StoreInstance;
import voldemort.versioning.Versioned;

import com.google.protobuf.Message;

/**
 * Fetches entries using an efficient partition scan. Of course, only works if
 * isPartitionScanSupported() is true for the storage engine to be scanned..
 * 
 */
public class PartitionScanFetchEntriesRequestHandler extends PartitionScanFetchStreamRequestHandler {

    protected ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entriesPartitionIterator;

    public PartitionScanFetchEntriesRequestHandler(FetchPartitionEntriesRequest request,
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

        entriesPartitionIterator = null;
    }

    @Override
    public StreamRequestHandlerState handleRequest(DataInputStream inputStream,
                                                   DataOutputStream outputStream)
            throws IOException {

        // process the next partition
        if(entriesPartitionIterator == null) {

            if(currentIndex == partitionList.size()) {
                return StreamRequestHandlerState.COMPLETE;
            }

            // find the next partition to scan
            boolean found = false;
            while(!found && (currentIndex < partitionList.size())) {
                currentPartition = new Integer(partitionList.get(currentIndex));
                currentReplicaType = new Integer(replicaTypeList.get(currentIndex));

                // Check the current node contains the partition as the
                // requested replicatype
                if(!fetchedPartitions.contains(currentPartition)
                   && StoreInstance.checkPartitionBelongsToNode(currentPartition,
                                                                currentReplicaType,
                                                                nodeId,
                                                                initialCluster,
                                                                storeDef)) {
                    found = true;
                    completedFetchingCurrentPartition();
                    entriesPartitionIterator = storageEngine.entries(currentPartition);
                    statusInfoMessage("Starting fetch entries");
                }
                currentIndex++;
            }
        } else {
            long startNs = System.nanoTime();
            // do a check before reading in case partition has 0 elements
            if(entriesPartitionIterator.hasNext()) {
                Pair<ByteArray, Versioned<byte[]>> entry = entriesPartitionIterator.next();
                ByteArray key = entry.getFirst();
                Versioned<byte[]> value = entry.getSecond();
                reportStorageOpTime(startNs);

                throttler.maybeThrottle(key.length());

                if(filter.accept(key, value)) {
                    recordFetched();

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

                accountForScanProgress("entries");
            }

            if(!entriesPartitionIterator.hasNext() || fetchedEnoughForCurrentPartition()) {
                // Finished current partition. Reset iterator. Info status.
                entriesPartitionIterator.close();
                entriesPartitionIterator = null;

                statusInfoMessage("Finished fetch keys");
                progressInfoMessage("Fetch entries (end of partition)");
            }
        }
        return StreamRequestHandlerState.WRITING;
    }

    @Override
    public final void close(DataOutputStream outputStream) throws IOException {
        if(null != entriesPartitionIterator)
            entriesPartitionIterator.close();
        super.close(outputStream);
    }
}
