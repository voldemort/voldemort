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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import voldemort.utils.Time;

import com.google.protobuf.Message;

/**
 * Fetches the keys using an efficient partition scan
 * 
 */
public class FetchPartitionKeysStreamRequestHandler extends FetchStreamRequestHandler {

    protected ClosableIterator<ByteArray> keysPartitionIterator;
    protected Set<Integer> fetchedPartitions;
    protected List<Integer> replicaTypeList;
    protected List<Integer> partitionList;
    protected Integer currentIndex;

    public FetchPartitionKeysStreamRequestHandler(FetchPartitionEntriesRequest request,
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
        fetchedPartitions = new HashSet<Integer>();
        replicaTypeList = new ArrayList<Integer>();
        partitionList = new ArrayList<Integer>();
        currentIndex = 0;
        keysPartitionIterator = null;

        // flatten the replicatype to partition map
        for(Integer replicaType: replicaToPartitionList.keySet()) {
            if(replicaToPartitionList.get(replicaType) != null) {
                for(Integer partitionId: replicaToPartitionList.get(replicaType)) {
                    partitionList.add(partitionId);
                    replicaTypeList.add(replicaType);
                }
            }
        }
    }

    @Override
    public StreamRequestHandlerState handleRequest(DataInputStream inputStream,
                                                   DataOutputStream outputStream)
            throws IOException {

        // process the next partition
        if(keysPartitionIterator == null) {
            if(currentIndex == partitionList.size() || counter >= recordsPerPartition) {
                // TODO: Make all .info messages consistent. "Records fetched"
                // instead of "Done fetching".
                logger.info("Done fetching  store " + storageEngine.getName() + " : " + counter
                            + " records processed.");
                return StreamRequestHandlerState.COMPLETE;
            }

            boolean found = false;
            // find the next partition to scan
            while(!found && (currentIndex < partitionList.size())) {
                Integer currentPartition = partitionList.get(currentIndex);
                Integer currentReplicaType = replicaTypeList.get(currentIndex);

                // Check the current node contains the partition as the
                // requested replicatype
                if(!fetchedPartitions.contains(currentPartition)
                   && StoreInstance.checkPartitionBelongsToNode(currentPartition,
                                                                currentReplicaType,
                                                                nodeId,
                                                                initialCluster,
                                                                storeDef)) {
                    fetchedPartitions.add(currentPartition);
                    found = true;
                    logger.info("Fetching [partition: " + currentPartition + ", replica type:"
                                + currentReplicaType + "] for store " + storageEngine.getName());
                    keysPartitionIterator = storageEngine.keys(currentPartition);
                }
                currentIndex++;
            }
        } else {
            long startNs = System.nanoTime();
            // do a check before reading in case partition has 0 elements
            if(keysPartitionIterator.hasNext()) {
                counter++;
                ByteArray key = keysPartitionIterator.next();

                // do the filtering
                if(streamStats != null) {
                    // TODO: The accounting for streaming reads should also
                    // move along with the next() call since we are indeed
                    // fetching from disk.. ---VChandar
                    streamStats.reportStorageTime(operation, System.nanoTime() - startNs);
                    streamStats.reportStreamingScan(operation);
                }
                throttler.maybeThrottle(key.length());
                if(filter.accept(key, null)) {

                    VAdminProto.FetchPartitionEntriesResponse.Builder response = VAdminProto.FetchPartitionEntriesResponse.newBuilder();
                    response.setKey(ProtoUtils.encodeBytes(key));

                    fetched++;
                    if(streamStats != null)
                        streamStats.reportStreamingFetch(operation);
                    Message message = response.build();

                    startNs = System.nanoTime();
                    ProtoUtils.writeMessage(outputStream, message);
                    if(streamStats != null)
                        streamStats.reportNetworkTime(operation, System.nanoTime() - startNs);
                }

                // log progress
                if(0 == counter % STAT_RECORDS_INTERVAL) {
                    long totalTime = (System.currentTimeMillis() - startTime) / Time.MS_PER_SECOND;

                    logger.info("Fetch entries scanned " + counter + " entries, fetched " + fetched
                                + " entries for store '" + storageEngine.getName()
                                + "' replicaToPartitionList:" + replicaToPartitionList + " in "
                                + totalTime + " s");
                }
            }

            // TODO: Add logic to FetchKeys and FetchEntries to count keys per
            // partition correctly.

            // reset the iterator if done with this partition or fetched enough
            // records
            if(!keysPartitionIterator.hasNext() || (counter >= recordsPerPartition)) {
                keysPartitionIterator.close();
                keysPartitionIterator = null;
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
