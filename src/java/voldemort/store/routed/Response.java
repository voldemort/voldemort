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

package voldemort.store.routed;

import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.nonblockingstore.NonblockingStoreCallback;
import voldemort.store.routed.Pipeline.Event;
import voldemort.utils.ByteArray;

/**
 * Response represents a response from a call to a remote Voldemort node to
 * perform some operation (get, put, etc.). It includes the {@link Node} and
 * request time as these are needed by the {@link FailureDetector} that will be
 * used by the user of the Response.
 * 
 * <p/>
 * 
 * A Response is usually used in conjunction with asynchronous requests as a
 * sort of <i>callback</i> mechanism, though this isn't always the case. In the
 * case where they are the result of asynchronous requests, the
 * {@link NonblockingStore} will invoke the {@link NonblockingStoreCallback}
 * instances's <code>requestComplete</code> method which will in turn package up
 * the data in a Response object that is sent to the {@link Pipeline} via an
 * {@link Event#RESPONSES_RECEIVED} event.
 * 
 * <p/>
 * 
 * Response instances are stored in the {@link PipelineData} to represent the
 * responses to requests made during execution of the {@link Pipeline}.
 * 
 * <p/>
 * 
 * This class uses generics for the value to support the return types used by
 * the different operations. Oftentimes the key type is simply a
 * {@link ByteArray} instance, but in the case of the "get all" operation, the
 * key is actually an {@link Iterable}.
 * 
 * @param <K> Type for the key used in the request
 * @param <V> Type for the value returned by the call
 * 
 * @see NonblockingStore
 * @see NonblockingStoreCallback
 * @see Pipeline
 * @see PipelineData
 */

public class Response<K, V> {

    private final Node node;

    private final K key;

    private final V value;

    private final long requestTime;

    public Response(Node node, K key, V value, long requestTime) {
        this.node = node;
        this.key = key;
        this.value = value;
        this.requestTime = requestTime;
    }

    public Node getNode() {
        return node;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    public long getRequestTime() {
        return requestTime;
    }

}
