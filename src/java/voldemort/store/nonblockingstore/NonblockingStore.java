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

import voldemort.VoldemortException;
import voldemort.utils.ByteArray;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public interface NonblockingStore {

    public void submitGetRequest(ByteArray key, NonblockingStoreCallback callback)
            throws VoldemortException;

    public void submitGetAllRequest(Iterable<ByteArray> keys, NonblockingStoreCallback callback)
            throws VoldemortException;

    public void submitGetVersionsRequest(ByteArray key, NonblockingStoreCallback callback);

    public void submitPutRequest(ByteArray key,
                                 Versioned<byte[]> value,
                                 NonblockingStoreCallback callback) throws VoldemortException;

    public void submitDeleteRequest(ByteArray key,
                                    Version version,
                                    NonblockingStoreCallback callback);

    public void close() throws VoldemortException;

}