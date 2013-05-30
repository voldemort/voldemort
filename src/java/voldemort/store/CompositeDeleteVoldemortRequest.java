/*
 * Copyright 2013 LinkedIn, Inc
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

import voldemort.common.VoldemortOpCode;
import voldemort.versioning.Version;

/**
 * A class that defines a composite delete request containing the key to delete,
 * corresponding version (if present in the incoming HTTP request) and the
 * timeout
 * 
 */
public class CompositeDeleteVoldemortRequest<K, V> extends CompositeVoldemortRequest<K, V> {

    public CompositeDeleteVoldemortRequest(K key, Version version, long timeoutInMs) {
        super(key, null, null, null, version, timeoutInMs, true, VoldemortOpCode.DELETE_OP_CODE);
    }
}
