package voldemort.server.scheduler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.store.Entry;
import voldemort.store.ObsoleteVersionException;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.store.slop.Slop;
import voldemort.store.slop.Slop.Operation;
import voldemort.utils.ClosableIterator;
import voldemort.versioning.Versioned;

/**
 * A task which goes through the slop table and attempts to push out all the
 * slop to its rightful owner node
 * 
 * @author jay
 * 
 */
public class SlopPusherJob implements Runnable {

    private static final Logger logger = Logger.getLogger(SlopPusherJob.class.getName());

    private final StorageEngine<byte[], Slop> slopStore;
    private final ConcurrentMap<Integer, Store<byte[], byte[]>> stores;

    public SlopPusherJob(StorageEngine<byte[], Slop> slop,
                      Map<Integer, ? extends Store<byte[], byte[]>> stores) {
        this.slopStore = slop;
        this.stores = new ConcurrentHashMap<Integer, Store<byte[], byte[]>>(stores);
    }

    /**
     * Loop over entries in the slop table and attempt to push them to the
     * deserving server
     */
    public void run() {
        logger.debug("Pushing slop...");
        int slopsPushed = 0;
        int attemptedPushes = 0;
        ClosableIterator<Entry<byte[], Versioned<Slop>>> iterator = null;
        try {
            iterator = slopStore.entries();
            while(iterator.hasNext()) {
                attemptedPushes++;
                if(Thread.currentThread().isInterrupted())
                    throw new InterruptedException("Task cancelled!");

                try {
                    Entry<byte[], Versioned<Slop>> entry = iterator.next();
                    Versioned<Slop> versioned = entry.getValue();
                    Slop slop = versioned.getValue();
                    Store<byte[], byte[]> store = stores.get(slop.getNodeId());
                    try {
                        if(slop.getOperation() == Operation.PUT)
                            store.put(entry.getKey(), new Versioned<byte[]>(slop.getValue(),
                                                                            versioned.getVersion()));
                        else
                            store.delete(entry.getKey(), versioned.getVersion());
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

    public void close() {
        this.slopStore.close();
    }

}