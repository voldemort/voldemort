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

/**
 * A class that defines a composite get request containing the key, a flag to
 * indicate whether the conflicts should be resolved and the timeout
 * 
 */

public class CompositeGetVoldemortRequest<K, V> extends CompositeVoldemortRequest<K, V> {

    public CompositeGetVoldemortRequest(K key, long timeoutInMs, boolean resolveConflicts) {
        super(key,
              null,
              null,
              null,
              null,
              timeoutInMs,
              resolveConflicts,
              VoldemortOpCode.GET_OP_CODE);
    }
}
