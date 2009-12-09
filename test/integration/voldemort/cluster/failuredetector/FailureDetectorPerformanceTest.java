/*
 * Copyright 2009 LinkedIn, Inc
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

import static voldemort.cluster.failuredetector.FailureDetectorUtils.create;
import voldemort.MockTime;
import voldemort.MutableStoreResolver;
import voldemort.cluster.Node;
import voldemort.utils.SystemTime;
import voldemort.utils.Time;

public abstract class FailureDetectorPerformanceTest {

    public abstract String test(FailureDetector failureDetector, Time time) throws Exception;

    protected String run(FailureDetectorConfig failureDetectorConfig) throws Exception {
        failureDetectorConfig.setTime(new MockTime(0));
        failureDetectorConfig.setTime(SystemTime.INSTANCE);

        Time time = failureDetectorConfig.getTime();
        FailureDetector failureDetector = create(failureDetectorConfig);

        try {
            return test(failureDetector, time);
        } finally {
            failureDetector.destroy();
        }
    }

    protected void updateNodeStoreAvailability(FailureDetectorConfig failureDetectorConfig,
                                               Node node,
                                               boolean shouldMarkAvailable) {
        ((MutableStoreResolver) failureDetectorConfig.getStoreResolver()).setReturnNullStore(node,
                                                                                             !shouldMarkAvailable);
    }

}
