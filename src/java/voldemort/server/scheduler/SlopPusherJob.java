/*
 * Copyright 2008-2009 LinkedIn, Inc
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

package voldemort.server.scheduler;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.server.StoreRepository;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.store.slop.Slop;
import voldemort.store.slop.Slop.Operation;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Versioned;

/**
 * A task which goes through the slop table and attempts to push out all the
 * slop to its rightful owner node
 * 
 * 
 */
public class SlopPusherJob implements Runnable {

    private static final Logger logger = Logger.getLogger(SlopPusherJob.class.getName());

    private final StoreRepository storeRepo;

    public SlopPusherJob(StoreRepository storeRepo) {
        this.storeRepo = storeRepo;
    }

    /**
     * Loop over entries in the slop table and attempt to push them to the
     * deserving server
     */
    public void run() {
        logger.debug("Pushing slop...");
        int slopsPushed = 0;
        int attemptedPushes = 0;
        ClosableIterator<Pair<ByteArray, Versioned<Slop>>> iterator = null;
        try {
            StorageEngine<ByteArray, Slop> slopStore = storeRepo.getSlopStore();
            iterator = slopStore.entries();
            while(iterator.hasNext()) {
                if(Thread.interrupted())
                    throw new InterruptedException("Task cancelled!");
                attemptedPushes++;

                try {
                    Pair<ByteArray, Versioned<Slop>> keyAndVal = iterator.next();
                    Versioned<Slop> versioned = keyAndVal.getSecond();
                    Slop slop = versioned.getValue();
                    Store<ByteArray, byte[]> store = storeRepo.getNodeStore(slop.getStoreName(),
                                                                            slop.getNodeId());
                    try {
                        if(slop.getOperation() == Operation.PUT)
                            store.put(keyAndVal.getFirst(),
                                      new Versioned<byte[]>(slop.getValue(), versioned.getVersion()));
                        else if(slop.getOperation() == Operation.DELETE)
                            store.delete(keyAndVal.getFirst(), versioned.getVersion());
                        else
                            logger.error("Unknown slop operation: " + slop.getOperation());
                        slopStore.delete(slop.makeKey(), versioned.getVersion());
                        slopsPushed++;
                    } catch(ObsoleteVersionException e) {
                        // okay it is old, just delete it
                        slopStore.delete(slop.makeKey(), versioned.getVersion());
                    }
                } catch(Exception e) {
                    logger.error(e);
                }
            }
        } catch(Exception e) {
            logger.error(e);
        } finally {
            try {
                if(iterator != null)
                    iterator.close();
            } catch(Exception e) {
                logger.error("Failed to close iterator.", e);
            }
        }

        // typically not useful to hear that 0 items were attempted so log as
        // debug
        logger.log(attemptedPushes > 0 ? Level.INFO : Level.DEBUG,
                   "Attempted " + attemptedPushes + " hinted handoff pushes of which "
                           + slopsPushed + " succeeded.");
    }

}
