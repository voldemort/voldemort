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

import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.routed.BasicPipelineData;
import voldemort.store.routed.ReadRepairer;
import voldemort.store.routed.Response;
import voldemort.store.routed.Pipeline.Event;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

public class ReadRepair<PD extends BasicPipelineData<List<Versioned<byte[]>>>> extends
        AbstractReadRepair<ByteArray, List<Versioned<byte[]>>, PD> {

    public ReadRepair(PD pipelineData,
                      Event completeEvent,
                      int preferred,
                      long timeoutMs,
                      Map<Integer, NonblockingStore> nonblockingStores,
                      ReadRepairer<ByteArray, byte[]> readRepairer) {
        super(pipelineData, completeEvent, preferred, timeoutMs, nonblockingStores, readRepairer);
    }

    @Override
    protected void insertNodeValues() {
        for(Response<ByteArray, List<Versioned<byte[]>>> response: pipelineData.getResponses())
            insertNodeValue(response.getNode(), response.getKey(), response.getValue());
    }

}
