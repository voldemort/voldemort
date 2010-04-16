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

import voldemort.store.routed.PipelineData;
import voldemort.store.routed.Pipeline.Event;

public abstract class AbstractKeyBasedAction<K, V, PD extends PipelineData<K, V>> extends
        AbstractAction<K, V, PD> {

    protected final K key;

    protected AbstractKeyBasedAction(PD pipelineData, Event completeEvent, K key) {
        super(pipelineData, completeEvent);
        this.key = key;
    }
}
