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
import voldemort.MutableStoreResolver;
import voldemort.cluster.Node;
import voldemort.utils.Time;

public abstract class FailureDetectorPerformanceTest {

    public long run(final FailureDetectorConfig failureDetectorConfig) throws Exception {
        Time time = failureDetectorConfig.getTime();
        PerformanceTestFailureDetectorListener listener = new PerformanceTestFailureDetectorListener(time);
        FailureDetector failureDetector = create(failureDetectorConfig, listener);
        test(failureDetector, listener, time);
        failureDetector.destroy();

        return listener.getDelta();
    }

    public abstract void test(FailureDetector failureDetector,
                              PerformanceTestFailureDetectorListener listener,
                              Time time) throws Exception;

    protected void updateNodeStoreAvailability(FailureDetectorConfig failureDetectorConfig,
                                               Node node,
                                               boolean shouldMarkAvailable) {
        ((MutableStoreResolver) failureDetectorConfig.getStoreResolver()).setReturnNullStore(node,
                                                                                             !shouldMarkAvailable);
    }

    protected static class PerformanceTestFailureDetectorListener implements
            FailureDetectorListener {

        private final Time time;

        private long markedAvailable;

        private long markedUnavailable;

        public PerformanceTestFailureDetectorListener(Time time) {
            this.time = time;
        }

        public void nodeAvailable(Node node) {
            markedAvailable = time.getMilliseconds();
        }

        public void nodeUnavailable(Node node) {
            markedUnavailable = time.getMilliseconds();
        }

        public long getDelta() {
            return markedAvailable - markedUnavailable;
        }

    }

}
