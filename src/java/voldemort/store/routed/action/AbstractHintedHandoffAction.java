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

import voldemort.cluster.Node;
import voldemort.store.routed.BasicPipelineData;
import voldemort.store.routed.Pipeline;
import voldemort.store.slop.HintedHandoff;
import voldemort.utils.ByteArray;

public abstract class AbstractHintedHandoffAction<V, PD extends BasicPipelineData<V>> extends
        AbstractKeyBasedAction<ByteArray, V, PD> {

    protected final List<Node> failedNodes;

    protected final HintedHandoff hintedHandoff;

    public AbstractHintedHandoffAction(PD pipelineData,
                                       Pipeline.Event completeEvent,
                                       ByteArray key,
                                       HintedHandoff hintedHandoff) {
        super(pipelineData, completeEvent, key);
        this.hintedHandoff = hintedHandoff;
        this.failedNodes = pipelineData.getFailedNodes();
    }

    public abstract void execute(Pipeline pipeline);
}
