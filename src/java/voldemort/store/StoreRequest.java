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

package voldemort.store;

import voldemort.VoldemortException;
import voldemort.store.nonblockingstore.NonblockingStoreRequest;
import voldemort.utils.ByteArray;

/**
 * A StoreRequest is a wrapper around one of the five existing requests to the
 * {@link Store} API. It is useful to provide a sort of <i>function object</i>
 * for various utility methods.
 * 
 * @param <T> Type returned by {@link Store} request
 * 
 * @see NonblockingStoreRequest
 */

public interface StoreRequest<T> {

    /**
     * Perform the request to the given {@link Store}. It is assumed that the
     * parameters for the call are part of the implementing object internally.
     * They are not provided here.
     * 
     * @param store {@link Store} on which to invoke the request
     * 
     * @return Value from underlying {@link Store}
     * 
     * @throws VoldemortException Thrown on errors invoking the operation on the
     *         given {@link Store}
     */

    public T request(Store<ByteArray, byte[], byte[]> store) throws VoldemortException;

}
