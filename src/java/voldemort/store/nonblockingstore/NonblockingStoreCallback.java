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

package voldemort.store.nonblockingstore;

/**
 * A NonblockingStoreCallback is provided as an argument to the submit methods
 * of the {@link NonblockingStore} API. When the response from the request is
 * received, the {@link #requestComplete} method is invoked with the result of
 * the request and the time (in milliseconds) it took between issuing the
 * request and receiving the response.
 * 
 * @see NonblockingStore
 */

public interface NonblockingStoreCallback {

    /**
     * Signals that the request is complete and provides the response -- or
     * Exception (if the request failed) -- and the time (in milliseconds) it
     * took between issuing the request and receiving the response.
     * 
     * @param result Type-specific result from the request or Exception if the
     *        request failed
     * @param requestTime Time (in milliseconds) for the duration of the request
     */

    public void requestComplete(Object result, long requestTime);

}