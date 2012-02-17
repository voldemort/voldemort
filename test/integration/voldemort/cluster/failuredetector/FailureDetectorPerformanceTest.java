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

package voldemort.cluster.failuredetector;

import static voldemort.cluster.failuredetector.MutableStoreVerifier.create;
import static voldemort.VoldemortTestConstants.getNineNodeCluster;
import static voldemort.cluster.failuredetector.FailureDetectorUtils.create;

import java.io.IOException;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.UnreachableStoreException;
import voldemort.utils.CmdUtils;

public abstract class FailureDetectorPerformanceTest {

    protected final FailureDetectorConfig failureDetectorConfig;

    protected FailureDetectorPerformanceTest(String[] args) {
        // We instantiate this way up here so we can use the defaults.
        this.failureDetectorConfig = new FailureDetectorConfig();

        OptionParser parser = new OptionParser();
        parser.accepts("async-scan-interval",
                       "Time interval (in milliseconds) at which the AsyncRecoveryFailureDetector checks for recovered nodes. Default: "
                               + failureDetectorConfig.getAsyncRecoveryInterval())
              .withRequiredArg()
              .ofType(Long.class);
        parser.accepts("bannage-period",
                       "Time period (in milliseconds) for which a failed node is marked unavailable for the BannagePeriodFailureDetector. Default: "
                               + failureDetectorConfig.getBannagePeriod())
              .withRequiredArg()
              .ofType(Long.class);
        parser.accepts("threshold-interval",
                       "Time interval (in milliseconds) for which a node is marked unavailable by the ThresholdFailureDetector for having fallen under the threshold for failures for the period, after which it is considered available. Default: "
                               + failureDetectorConfig.getThresholdInterval())
              .withRequiredArg()
              .ofType(Long.class);
        parser.accepts("help", "This help");

        OptionSet options = parser.parse(args);

        if(options.has("help"))
            printUsage(parser);

        Long asyncScanInterval = CmdUtils.valueOf(options,
                                                  "async-scan-interval",
                                                  failureDetectorConfig.getAsyncRecoveryInterval());
        Long bannagePeriod = CmdUtils.valueOf(options,
                                              "bannage-period",
                                              failureDetectorConfig.getBannagePeriod());
        Long thresholdInterval = CmdUtils.valueOf(options,
                                                  "threshold-interval",
                                                  failureDetectorConfig.getThresholdInterval());
        Cluster cluster = getNineNodeCluster();

        failureDetectorConfig.setNodes(cluster.getNodes())
                             .setStoreVerifier(create(cluster.getNodes()))
                             .setAsyncRecoveryInterval(asyncScanInterval)
                             .setBannagePeriod(bannagePeriod)
                             .setThresholdInterval(thresholdInterval);
    }

    protected abstract String getTestHeaders();

    public abstract String test(FailureDetector failureDetector) throws Exception;

    protected void printUsage(OptionParser parser) {
        System.err.println("Usage: $VOLDEMORT_HOME/bin/run-class.sh " + getClass().getName()
                           + " [options]\n");

        try {
            parser.printHelpOn(System.err);
        } catch(IOException e) {
            e.printStackTrace();
        }

        System.exit(1);
    }

    protected Class<?>[] getClasses() {
        return new Class<?>[] { AsyncRecoveryFailureDetector.class,
                BannagePeriodFailureDetector.class, ThresholdFailureDetector.class };
    }

    protected void test() {
        System.out.println(getTestHeaders());

        for(Class<?> implClass: getClasses()) {
            failureDetectorConfig.setImplementationClassName(implClass.getName());
            String result = null;

            try {
                FailureDetector failureDetector = create(failureDetectorConfig, false);

                try {
                    result = test(failureDetector);
                } finally {
                    failureDetector.destroy();
                }
            } catch(Exception e) {
                result = "ERROR: " + e.getMessage();
            }

            System.out.println(result);
        }
    }

    protected void updateNodeStoreAvailability(FailureDetectorConfig failureDetectorConfig,
                                               Node node,
                                               boolean shouldMarkAvailable) {
        UnreachableStoreException e = shouldMarkAvailable ? null
                                                         : new UnreachableStoreException("test error");
        ((MutableStoreVerifier) failureDetectorConfig.getStoreVerifier()).setErrorStore(node, e);
    }

}
