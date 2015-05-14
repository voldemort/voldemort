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
import voldemort.server.RequestRoutingType;
import voldemort.versioning.Versioned;

/**
 * A class that defines a composite put request containing the key, the
 * versioned value, the timeout, routing type and origin time
 * 
 */

public class CompositeVersionedPutVoldemortRequest<K, V> extends CompositeVoldemortRequest<K, V> {

    public CompositeVersionedPutVoldemortRequest(K key, Versioned<V> value, long timeoutInMs) {
        super(key, null, null, value, null, timeoutInMs, true, VoldemortOpCode.PUT_OP_CODE);
    }

    public CompositeVersionedPutVoldemortRequest(K key,
                                                 Versioned<V> value,
                                                 long timeoutInMs,
                                                 long originTimeInMs,
                                                 RequestRoutingType routingType) {
        super(key,
              null,
              null,
              value,
              null,
              timeoutInMs,
              false,
              VoldemortOpCode.PUT_OP_CODE,
              originTimeInMs,
              routingType);
    }

}
