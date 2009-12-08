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

import static voldemort.MutableStoreResolver.createMutableStoreResolver;
import static voldemort.VoldemortTestConstants.getNineNodeCluster;
import voldemort.MockTime;
import voldemort.cluster.Cluster;

public class FailureDetectorPerformanceTestHarness {

    public static void main(String[] args) throws Throwable {
        Cluster cluster = getNineNodeCluster();

        FailureDetectorConfig failureDetectorConfig = new FailureDetectorConfig().setNodes(cluster.getNodes())
                                                                                 .setStoreResolver(createMutableStoreResolver(cluster.getNodes()))
                                                                                 .setAsyncScanInterval(5000)
                                                                                 .setNodeBannagePeriod(5000);

        Class<?>[] classes = new Class[] { AsyncRecoveryFailureDetector.class,
                BannagePeriodFailureDetector.class, ThresholdFailureDetector.class };

        for(Class<?> implClass: classes) {
            failureDetectorConfig.setImplementationClassName(implClass.getName());

            run(new TimedUnavailabilityTest(2522), failureDetectorConfig);
            run(new TimedUnavailabilityTest(30000), failureDetectorConfig);
            run(new TimedUnavailabilityTest(59999), failureDetectorConfig);
        }
    }

    private static void run(FailureDetectorPerformanceTest test,
                            FailureDetectorConfig failureDetectorConfig) {
        try {
            failureDetectorConfig.setTime(new MockTime(0));
            // failureDetectorConfig.setTime(SystemTime.INSTANCE);
            long delta = test.run(failureDetectorConfig);

            System.out.println(test.getClass().getSimpleName()
                               + ", "
                               + Class.forName(failureDetectorConfig.getImplementationClassName())
                                      .getSimpleName() + ", " + delta);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

}
