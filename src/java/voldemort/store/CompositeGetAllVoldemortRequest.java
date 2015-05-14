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

/**
 * A class that defines a composite get all request containing a reference to
 * the iterable keys, a flag to indicate if the conflicts should be resolved and
 * the timeout, routing type and origin time
 * 
 */
public class CompositeGetAllVoldemortRequest<K, V> extends CompositeVoldemortRequest<K, V> {

    public CompositeGetAllVoldemortRequest(Iterable<K> keys,
                                           long timeoutInMs,
                                           boolean resolveConflicts) {
        super(null,
              null,
              keys,
              null,
              null,
              timeoutInMs,
              resolveConflicts,
              VoldemortOpCode.GET_ALL_OP_CODE);
    }

    // RestServerErrorHandler uses this constructor
    public CompositeGetAllVoldemortRequest(Iterable<K> keys,
                                           long timeoutInMs,
                                           long originTimeInMs,
                                           RequestRoutingType routingType) {
        super(null,
              null,
              keys,
              null,
              null,
              timeoutInMs,
              false,
              VoldemortOpCode.GET_ALL_OP_CODE,
              originTimeInMs,
              routingType);

    }
}
