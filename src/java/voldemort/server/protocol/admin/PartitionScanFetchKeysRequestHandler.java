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
import voldemort.utils.StoreInstance;

import com.google.protobuf.Message;

/**
 * Fetches keys using an efficient partition scan. Of course, only works if
 * isPartitionScanSupported() is true for the storage engine to be scanned..
 * 
 */
public class PartitionScanFetchKeysRequestHandler extends PartitionScanFetchStreamRequestHandler {

    protected ClosableIterator<ByteArray> keysPartitionIterator;

    public PartitionScanFetchKeysRequestHandler(FetchPartitionEntriesRequest request,
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
        logger.info("Starting fetch keys for store '" + storageEngine.getName()
                    + "' with replica to partition mapping " + replicaToPartitionList);

        keysPartitionIterator = null;
    }

    @Override
    public StreamRequestHandlerState handleRequest(DataInputStream inputStream,
                                                   DataOutputStream outputStream)
            throws IOException {

        // process the next partition
        if(keysPartitionIterator == null) {

            if(currentIndex == partitionList.size()) {
                return StreamRequestHandlerState.COMPLETE;
            }

            // find the next partition to scan and set currentIndex.
            boolean found = false;
            while(!found && (currentIndex < partitionList.size())) {
                currentPartition = partitionList.get(currentIndex);
                currentReplicaType = replicaTypeList.get(currentIndex);

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
                    keysPartitionIterator = storageEngine.keys(currentPartition);
                    statusInfoMessage("Starting fetch keys");
                }
                currentIndex++;
            }
        } else {
            long startNs = System.nanoTime();
            // do a check before reading in case partition has 0 elements
            if(keysPartitionIterator.hasNext()) {
                ByteArray key = keysPartitionIterator.next();
                reportStorageOpTime(startNs);

                throttler.maybeThrottle(key.length());

                if(filter.accept(key, null)) {
                    recordFetched();

                    VAdminProto.FetchPartitionEntriesResponse.Builder response = VAdminProto.FetchPartitionEntriesResponse.newBuilder();
                    response.setKey(ProtoUtils.encodeBytes(key));
                    Message message = response.build();

                    sendMessage(outputStream, message);
                }

                accountForScanProgress("keys");
            }

            if(!keysPartitionIterator.hasNext() || fetchedEnoughForCurrentPartition()) {
                // Finished current partition. Reset iterator. Info status.
                keysPartitionIterator.close();
                keysPartitionIterator = null;

                statusInfoMessage("Finished fetch keys");
                progressInfoMessage("Fetch keys (end of partition)");
            }
        }
        return StreamRequestHandlerState.WRITING;
    }

    @Override
    public final void close(DataOutputStream outputStream) throws IOException {
        if(null != keysPartitionIterator)
            keysPartitionIterator.close();
        super.close(outputStream);
    }
}
