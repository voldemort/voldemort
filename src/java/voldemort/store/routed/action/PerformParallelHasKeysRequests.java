/*
 * Copyright 2010 LinkedIn, Inc
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

package voldemort.store.routed.action;

import java.util.Collection;
import java.util.Map;

import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.nonblockingstore.NonblockingStoreCallback;
import voldemort.store.routed.MultiKeysPipelineData;
import voldemort.store.routed.Pipeline.Event;
import voldemort.utils.ByteArray;

public class PerformParallelHasKeysRequests extends PerformParallelMultiKeysRequests<Boolean> {

    private final boolean exact;

    public PerformParallelHasKeysRequests(MultiKeysPipelineData<Boolean> pipelineData,
                                          Event completeEvent,
                                          FailureDetector failureDetector,
                                          long timeoutMs,
                                          Map<Integer, NonblockingStore> nonblockingStores,
                                          boolean exact) {
        super(pipelineData, completeEvent, failureDetector, timeoutMs, nonblockingStores);
        this.exact = exact;
    }

    @Override
    public void transform(ByteArray key, Map<ByteArray, Boolean> values) {
        Boolean retrieved = values.get(key);
        if(retrieved == null)
            retrieved = new Boolean(false);

        Boolean existing = pipelineData.getResult().get(key);
        if(existing == null)
            pipelineData.getResult().put(key, retrieved);
        else
            pipelineData.getResult().put(key, existing | retrieved);

    }

    @Override
    public void submitRequest(NonblockingStore store,
                              NonblockingStoreCallback callback,
                              Collection<ByteArray> keys) {
        store.submitHasKeysRequest(keys, exact, callback, timeoutMs);
    }
}