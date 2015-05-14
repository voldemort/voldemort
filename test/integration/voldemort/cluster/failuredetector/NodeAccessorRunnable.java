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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import voldemort.cluster.Node;
import voldemort.store.UnreachableStoreException;
import voldemort.utils.Time;

public class NodeAccessorRunnable implements Runnable {

    private final FailureDetector failureDetector;

    private final Node node;

    private final CountDownLatch countDownLatch;

    private final AtomicInteger successCounter;

    private final AtomicInteger failureCounter;

    private final UnreachableStoreException exception;

    private final long pollInterval;

    private final long failureDelay;

    public NodeAccessorRunnable(FailureDetector failureDetector,
                                Node node,
                                CountDownLatch countDownLatch,
                                AtomicInteger successCounter,
                                AtomicInteger failureCounter,
                                UnreachableStoreException exception,
                                long pollInterval,
                                long failureDelay) {
        this.failureDetector = failureDetector;
        this.node = node;
        this.countDownLatch = countDownLatch;
        this.successCounter = successCounter;
        this.failureCounter = failureCounter;
        this.exception = exception;
        this.pollInterval = pollInterval;
        this.failureDelay = failureDelay;
    }

    public void run() {
        FailureDetectorConfig failureDetectorConfig = failureDetector.getConfig();

        try {
            while(countDownLatch.getCount() > 0) {
                if(failureDetector.isAvailable(node)) {
                    long startNs = System.nanoTime();
                    try {
                        failureDetectorConfig.getConnectionVerifier().verifyConnection(node);
                        failureDetector.recordSuccess(node,
                                                      ((System.nanoTime() - startNs) / Time.NS_PER_MS));

                        if(successCounter != null)
                            successCounter.incrementAndGet();
                    } catch(UnreachableStoreException e) {
                        failureDetectorConfig.getTime().sleep(failureDelay);
                        failureDetector.recordException(node,
                                                        ((System.nanoTime() - startNs) / Time.NS_PER_MS),
                                                        exception);

                        if(failureCounter != null)
                            failureCounter.incrementAndGet();
                    }
                }

                failureDetectorConfig.getTime().sleep(pollInterval);
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

}
