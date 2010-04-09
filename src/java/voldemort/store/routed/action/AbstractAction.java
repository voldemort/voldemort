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

import org.apache.log4j.Logger;

import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.Store;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.routed.StateData;
import voldemort.store.routed.StateMachine.Event;
import voldemort.utils.ByteArray;
import voldemort.utils.Time;

public abstract class AbstractAction<T extends StateData> implements Action {

    protected T stateData;

    protected FailureDetector failureDetector;

    protected int preferred;

    protected int required;

    protected Map<Integer, NonblockingStore> nonblockingStores;

    protected Map<Integer, Store<ByteArray, byte[]>> stores;

    protected Event completeEvent;

    protected Time time;

    protected final Logger logger = Logger.getLogger(getClass());

    public T getStateData() {
        return stateData;
    }

    public void setStateData(T stateData) {
        this.stateData = stateData;
    }

    public FailureDetector getFailureDetector() {
        return failureDetector;
    }

    public void setFailureDetector(FailureDetector failureDetector) {
        this.failureDetector = failureDetector;
    }

    public int getPreferred() {
        return preferred;
    }

    public void setPreferred(int preferred) {
        this.preferred = preferred;
    }

    public int getRequired() {
        return required;
    }

    public void setRequired(int required) {
        this.required = required;
    }

    public Map<Integer, NonblockingStore> getNonblockingStores() {
        return nonblockingStores;
    }

    public void setNonblockingStores(Map<Integer, NonblockingStore> nonblockingStores) {
        this.nonblockingStores = nonblockingStores;
    }

    public Map<Integer, Store<ByteArray, byte[]>> getStores() {
        return stores;
    }

    public void setStores(Map<Integer, Store<ByteArray, byte[]>> stores) {
        this.stores = stores;
    }

    public Event getCompleteEvent() {
        return completeEvent;
    }

    public void setCompleteEvent(Event completeEvent) {
        this.completeEvent = completeEvent;
    }

    public Time getTime() {
        return time;
    }

    public void setTime(Time time) {
        this.time = time;
    }

}
