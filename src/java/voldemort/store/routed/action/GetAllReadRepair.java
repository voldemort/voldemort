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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.routed.GetAllPipelineData;
import voldemort.store.routed.ReadRepairer;
import voldemort.store.routed.Response;
import voldemort.store.routed.Pipeline.Event;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

public class GetAllReadRepair
        extends
        AbstractReadRepair<Iterable<ByteArray>, Map<ByteArray, List<Versioned<byte[]>>>, GetAllPipelineData> {

    public GetAllReadRepair(GetAllPipelineData pipelineData,
                            Event completeEvent,
                            int preferred,
                            long timeoutMs,
                            Map<Integer, NonblockingStore> nonblockingStores,
                            ReadRepairer<ByteArray, byte[]> readRepairer) {
        super(pipelineData, completeEvent, preferred, timeoutMs, nonblockingStores, readRepairer);
    }

    @Override
    protected void insertNodeValues() {
        for(Response<Iterable<ByteArray>, Map<ByteArray, List<Versioned<byte[]>>>> response: pipelineData.getResponses()) {
            Map<ByteArray, List<Versioned<byte[]>>> responseValue = response.getValue();

            for(Map.Entry<ByteArray, List<Versioned<byte[]>>> entry: responseValue.entrySet()) {
                ByteArray key = entry.getKey();
                List<Versioned<byte[]>> value = entry.getValue();
                insertNodeValue(response.getNode(), key, value);
            }

            for(ByteArray key: response.getKey()) {
                if(!responseValue.containsKey(key)) {
                    insertNodeValue(response.getNode(),
                                    key,
                                    Collections.<Versioned<byte[]>> emptyList());
                }
            }
        }
    }

}