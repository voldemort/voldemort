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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.nonblockingstore.NonblockingStoreCallback;
import voldemort.store.routed.MultiKeysPipelineData;
import voldemort.store.routed.Pipeline.Event;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;

public class PerformParallelGetAllRequests extends
        PerformParallelMultiKeysRequests<List<Versioned<byte[]>>> {

    public PerformParallelGetAllRequests(MultiKeysPipelineData<List<Versioned<byte[]>>> pipelineData,
                                         Event completeEvent,
                                         FailureDetector failureDetector,
                                         long timeoutMs,
                                         Map<Integer, NonblockingStore> nonblockingStores) {
        super(pipelineData, completeEvent, failureDetector, timeoutMs, nonblockingStores);
    }

    @Override
    public void submitRequest(NonblockingStore store,
                              NonblockingStoreCallback callback,
                              Collection<ByteArray> keys) {
        store.submitGetAllRequest(keys, pipelineData.getTransforms(), callback, timeoutMs);
    }

    @Override
    public void transform(ByteArray key, Map<ByteArray, List<Versioned<byte[]>>> values) {
        List<Versioned<byte[]>> retrieved = values.get(key);
        if(retrieved == null) {
            retrieved = new ArrayList<Versioned<byte[]>>();
        }

        List<Versioned<byte[]>> existing = pipelineData.getResult().get(key);

        if(existing == null)
            pipelineData.getResult().put(key, Lists.newArrayList(retrieved));
        else
            existing.addAll(retrieved);
    }
}