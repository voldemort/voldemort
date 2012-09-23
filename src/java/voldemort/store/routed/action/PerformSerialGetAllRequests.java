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

import java.util.List;
import java.util.Map;

import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.Store;
import voldemort.store.routed.MultiKeysPipelineData;
import voldemort.store.routed.Pipeline.Event;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;

public class PerformSerialGetAllRequests extends
        PerformSerialMultiKeysRequests<List<Versioned<byte[]>>> {

    public PerformSerialGetAllRequests(MultiKeysPipelineData<List<Versioned<byte[]>>> pipelineData,
                                       Event completeEvent,
                                       Iterable<ByteArray> keys,
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
    }

    @Override
    public List<Versioned<byte[]>> transform(Store<ByteArray, byte[], byte[]> store,
                                             Map<ByteArray, List<Versioned<byte[]>>> result,
                                             ByteArray key) {
        List<Versioned<byte[]>> values;
        if(pipelineData.getTransforms() == null)
            values = store.get(key, null);
        else
            values = store.get(key, pipelineData.getTransforms().get(key));

        if(result.get(key) == null)
            result.put(key, Lists.newArrayList(values));
        else
            result.get(key).addAll(values);
        return values;
    }

}
