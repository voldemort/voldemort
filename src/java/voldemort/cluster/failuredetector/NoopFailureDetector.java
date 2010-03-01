/*
 * Copyright 2009 Mustard Grain, Inc., 2009-2010 LinkedIn, Inc.
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

import voldemort.cluster.Node;
import voldemort.store.UnreachableStoreException;

/**
 * NoopFailureDetector is used for testing classes which don't actually need a
 * working FailureDetector ;)
 * 
 */

public class NoopFailureDetector implements FailureDetector {

    public long getLastChecked(Node node) {
        return -1;
    }

    public boolean isAvailable(Node node) {
        return true;
    }

    public FailureDetectorConfig getConfig() {
        return null;
    }

    public void recordException(Node node, long requestTime, UnreachableStoreException e) {}

    public void recordSuccess(Node node, long requestTime) {}

    public void addFailureDetectorListener(FailureDetectorListener failureDetectorListener) {}

    public void removeFailureDetectorListener(FailureDetectorListener failureDetectorListener) {}

    public int getAvailableNodeCount() {
        return -1;
    }

    public int getNodeCount() {
        return -1;
    }

    public void waitForAvailability(Node node) {}

    public void destroy() {}

}
