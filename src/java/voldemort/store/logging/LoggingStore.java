package voldemort.store.logging;

import java.util.List;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.store.DelegatingStore;
import voldemort.store.Store;
import voldemort.utils.SystemTime;
import voldemort.utils.Time;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A store wrapper that handles debug logging.
 * 
 * @author jay
 * 
 */
public class LoggingStore<K, V> extends DelegatingStore<K, V> {

    private final Logger logger;
    private final Time time;
    private final String storeType;

    public LoggingStore(Store<K, V> store) {
        this(store, new SystemTime());
    }

    public LoggingStore(Store<K, V> store, Time time) {
        super(store);
        this.logger = Logger.getLogger(store.getClass());
        this.time = time;
        String name = store.getClass().getName();
        this.storeType = name.substring(Math.max(0, name.lastIndexOf('.')), name.length());
    }

    @Override
    public void close() throws VoldemortException {
        if (logger.isDebugEnabled())
            logger.debug("Closing " + getName() + ".");
        super.close();
    }

    @Override
    public boolean delete(K key, Version version) throws VoldemortException {
        long startTimeNs = 0;
        boolean succeeded = false;
        if (logger.isDebugEnabled())
            startTimeNs = time.getNanoseconds();
        try {
            boolean deletedSomething = getInnerStore().delete(key, version);
            succeeded = true;
            return deletedSomething;
        } finally {
            if (logger.isDebugEnabled()) {
                double elapsedMs = (time.getNanoseconds() - startTimeNs) / (double) Time.NS_PER_MS;
                logger.debug("DELETE from store '" + getName() + "' completed "
                             + (succeeded ? "successfully" : "unsuccessfully") + " in " + elapsedMs
                             + " ms.");
            }
        }
    }

    @Override
    public List<Versioned<V>> get(K key) throws VoldemortException {
        long startTimeNs = 0;
        boolean succeeded = false;
        if (logger.isDebugEnabled())
            startTimeNs = time.getNanoseconds();
        try {
            List<Versioned<V>> l = getInnerStore().get(key);
            succeeded = true;
            return l;
        } finally {
            if (logger.isDebugEnabled()) {
                double elapsedMs = (time.getNanoseconds() - startTimeNs) / (double) Time.NS_PER_MS;
                logger.debug("GET from store '" + getName() + "' completed "
                             + (succeeded ? "successfully" : "unsuccessfully") + " in " + elapsedMs
                             + " ms.");
            }
        }
    }

    @Override
    public void put(K key, Versioned<V> value) throws VoldemortException {
        long startTimeNs = 0;
        boolean succeeded = false;
        if (logger.isDebugEnabled()) {
            startTimeNs = time.getNanoseconds();
        }
        try {
            getInnerStore().put(key, value);
            succeeded = true;
        } finally {
            if (logger.isDebugEnabled()) {
                double elapsedMs = (time.getNanoseconds() - startTimeNs) / (double) Time.NS_PER_MS;
                logger.debug("PUT from store '" + getName() + "' completed "
                             + (succeeded ? "successfully" : "unsuccessfully") + " in " + elapsedMs
                             + " ms.");
            }
        }
    }

}
