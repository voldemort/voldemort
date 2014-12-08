/*
 * Copyright 2009 Mustard Grain, Inc.
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

package voldemort.cluster.failuredetector;

import voldemort.VoldemortException;
import voldemort.cluster.Node;
import voldemort.store.UnreachableStoreException;

/**
 * A ConnectionVerifier is used to test node connectivity. The act of testing
 * node connectivity is not the same depending on your environment (in the
 * server itself, on a regular client or admin client (and even further it
 * depends on what transport is used for connecting to the server), unit tests,
 * etc.). This helps to extract away the differences.
 * 
 * <p/>
 * 
 * This is used by some FailureDetector implementations to attempt contact with
 * the node before marking said node as available.
 * 
 */

public interface ConnectionVerifier {

    /**
     * Verifies the ability to connect to a Store for this node.
     * 
     * @param node Node to test
     */

    public void verifyConnection(Node node) throws UnreachableStoreException, VoldemortException;

    /**
     * Flushes the cached stores if any
     */
    public void flushCachedStores();

}
