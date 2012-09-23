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

import java.util.Map;

import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.Store;
import voldemort.store.routed.MultiKeysPipelineData;
import voldemort.store.routed.Pipeline.Event;
import voldemort.utils.ByteArray;

import com.google.common.collect.Lists;

public class PerformSerialHasKeysRequests extends PerformSerialMultiKeysRequests<Boolean> {

    private final boolean exact;

    public PerformSerialHasKeysRequests(MultiKeysPipelineData<Boolean> pipelineData,
                                        Event completeEvent,
                                        Iterable<ByteArray> keys,
                                        boolean exact,
                                        FailureDetector failureDetector,
                                        Map<Integer, Store<ByteArray, byte[], byte[]>> stores,
                                        int preferred,
                                        int required,
                                        boolean allowPartial) {
        super(pipelineData,
              completeEvent,
              keys,
              failureDetector,
              stores,
              preferred,
              required,
              allowPartial);
        this.exact = exact;
    }

    @Override
    public Boolean transform(Store<ByteArray, byte[], byte[]> store,
                             Map<ByteArray, Boolean> result,
                             ByteArray key) {
        Map<ByteArray, Boolean> values = store.hasKeys(Lists.newArrayList(key), exact);

        Boolean retrieved = values.get(key);
        if(retrieved == null)
            retrieved = false;

        Boolean existing = result.get(key);
        if(existing == null)
            result.put(key, retrieved);
        else
            result.put(key, retrieved | existing);
        return retrieved;
    }

}
