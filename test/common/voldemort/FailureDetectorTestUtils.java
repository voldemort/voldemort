/*
 * Copyright 2009-2010 LinkedIn, Inc
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

package voldemort;

import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.store.UnreachableStoreException;

public class FailureDetectorTestUtils {

    public static void recordException(FailureDetector failureDetector, Node node) {
        recordException(failureDetector, node, 0, null);
    }

    public static void recordException(FailureDetector failureDetector,
                                       Node node,
                                       long requestTime,
                                       UnreachableStoreException e) {
        ((MutableStoreVerifier) failureDetector.getConfig().getStoreVerifier()).setErrorStore(node,
                                                                                              new UnreachableStoreException("test error"));
        failureDetector.recordException(node, requestTime, e);
    }

    public static void recordSuccess(FailureDetector failureDetector, Node node) throws Exception {
        recordSuccess(failureDetector, node, 0, true);
    }

    public static void recordSuccess(FailureDetector failureDetector,
                                     Node node,
                                     long requestTime,
                                     boolean shouldWait) throws Exception {
        ((MutableStoreVerifier) failureDetector.getConfig().getStoreVerifier()).setErrorStore(node,
                                                                                              null);
        failureDetector.recordSuccess(node, requestTime);

        if(shouldWait)
            failureDetector.waitForAvailability(node);
    }

}
