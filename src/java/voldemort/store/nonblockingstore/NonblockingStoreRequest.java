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

import voldemort.cluster.Node;
import voldemort.store.StoreRequest;

/**
 * A NonblockingStoreRequest is a wrapper around one of the five existing
 * requests to the {@link NonblockingStore} API. It is useful to provide a sort
 * of <i>function object</i> for various utility methods.
 * 
 * @see NonblockingStore
 * @see StoreRequest
 */

public interface NonblockingStoreRequest {

    /**
     * Submits the request to the given {@link NonblockingStore}. It is assumed
     * that the parameters for the call are part of the implementing object
     * internally. They are not provided here.
     * 
     * @param store {@link NonblockingStore} on which to submit the request
     */

    public void submit(Node node, NonblockingStore store, NonblockingStoreCallback callback);

}
