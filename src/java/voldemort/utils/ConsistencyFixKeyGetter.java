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

package voldemort.utils;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import voldemort.utils.ConsistencyFix.BadKeyInput;
import voldemort.utils.ConsistencyFix.BadKeyResult;
import voldemort.utils.ConsistencyFix.Status;

// TODO: Move (back) into ConsistencyFix?
class ConsistencyFixKeyGetter implements Runnable {

    // TODO: Add stats shared across all getters (invocations, successes,
    // etc.)

    private final CountDownLatch latch;
    private final ConsistencyFix consistencyFix;
    private final ExecutorService repairPuttersService;
    private final BlockingQueue<BadKeyInput> badKeyQIn;
    private final BlockingQueue<BadKeyResult> badKeyQOut;
    private final boolean verbose;

    ConsistencyFixKeyGetter(CountDownLatch latch,
                            ConsistencyFix consistencyFix,
                            ExecutorService repairPuttersService,
                            BlockingQueue<BadKeyInput> badKeyQIn,
                            BlockingQueue<BadKeyResult> badKeyQOut,
                            boolean verbose) {
        this.latch = latch;
        this.consistencyFix = consistencyFix;
        this.repairPuttersService = repairPuttersService;
        this.badKeyQIn = badKeyQIn;
        this.badKeyQOut = badKeyQOut;
        this.verbose = verbose;
    }

    private String myName() {
        return Thread.currentThread().getName();
    }

    @Override
    public void run() {
        int counter = 0;
        BadKeyInput badKeyInput = null;

        try {
            badKeyInput = badKeyQIn.take();

            while(!badKeyInput.isPoison()) {
                counter++;
                ConsistencyFix.doKeyGetStatus doKeyGetStatus = consistencyFix.doKeyGet(badKeyInput.getKey(),
                                                                                       verbose);

                if(doKeyGetStatus.status == Status.SUCCESS) {
                    repairPuttersService.submit(new ConsistencyFixRepairPutter(consistencyFix,
                                                                               badKeyInput.getKey(),
                                                                               badKeyQOut,
                                                                               doKeyGetStatus.nodeValues,
                                                                               verbose));
                } else {
                    badKeyQOut.put(consistencyFix.new BadKeyResult(badKeyInput.getKey(),
                                                                   doKeyGetStatus.status));
                }

                badKeyInput = badKeyQIn.take();
            }

            // Done. Poison other KeyGetters!
            badKeyQIn.put(consistencyFix.new BadKeyInput());
        } catch(InterruptedException ie) {
            System.err.println("KeyGetter thread " + myName() + " interruped.");
        } finally {
            latch.countDown();
        }
        System.err.println("Thread " + myName() + " has swallowed poison and has counter = "
                           + counter);
    }

}