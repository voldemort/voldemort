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
import voldemort.utils.NetworkClassLoader;

import com.google.protobuf.Message;

/**
 * Fetches keys by scanning entire storage engine in storage-order.
 * 
 */
public class FullScanFetchKeysRequestHandler extends FullScanFetchStreamRequestHandler {

    public FullScanFetchKeysRequestHandler(FetchPartitionEntriesRequest request,
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
        long startNs = System.nanoTime();
        ByteArray key = keyIterator.next();
        reportStorageOpTime(startNs);

        throttler.maybeThrottle(key.length());

        if(isItemAccepted(key.get())) {
            if(filter.accept(key, null)) {
                accountForFetchedKey(key.get());

                VAdminProto.FetchPartitionEntriesResponse.Builder response = VAdminProto.FetchPartitionEntriesResponse.newBuilder();
                response.setKey(ProtoUtils.encodeBytes(key));
                Message message = response.build();

                sendMessage(outputStream, message);
            }
        }

        accountForScanProgress("keys");

        return determineRequestHandlerState("keys");
    }
}
