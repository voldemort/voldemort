package voldemort.store.socket;

import org.apache.commons.pool.KeyedObjectPool;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.store.UnreachableStoreException;

/**
 * A pool of sockets keyed off the socket destination. This wrapper just
 * translates exceptions and delegates to commons pool
 * 
 * @author jay
 * 
 */
public class SocketPool {

    private static final Logger logger = Logger.getLogger(SocketPool.class);

    private final KeyedObjectPool pool;
    private final SocketPoolableObjectFactory objFactory;

    public SocketPool(int maxConnectionsPerNode, int maxTotalConnections, int timeoutMs) {
        GenericKeyedObjectPool.Config config = new GenericKeyedObjectPool.Config();
        config.maxActive = maxConnectionsPerNode;
        config.maxTotal = maxTotalConnections;
        config.maxIdle = maxTotalConnections;
        config.maxWait = timeoutMs;
        config.testOnBorrow = true;
        config.testOnReturn = true;
        config.minEvictableIdleTimeMillis = 200 * 1000;
        config.whenExhaustedAction = GenericKeyedObjectPool.WHEN_EXHAUSTED_BLOCK;
        this.objFactory = new SocketPoolableObjectFactory(timeoutMs);
        this.pool = new GenericKeyedObjectPool(objFactory, config);
    }

    public SocketAndStreams checkout(SocketDestination destination) {
        try {
            return (SocketAndStreams) pool.borrowObject(destination);
        } catch (Exception e) {
            throw new UnreachableStoreException("Failure while checking out socket for "
                                                + destination + ": ", e);
        }
    }

    public void checkin(SocketDestination destination, SocketAndStreams socket) {
        try {
            pool.returnObject(destination, socket);
        } catch (Exception e) {
            try {
                pool.invalidateObject(destination, socket);
            } catch (Exception e2) {
                logger.error("Error while destroying socket:", e2);
            }
            throw new UnreachableStoreException("Failure while checking out socket for "
                                                + destination + ": ", e);
        }
    }

    public void close() {
        try {
            pool.clear();
            pool.close();
        } catch (Exception e) {
            throw new VoldemortException(e);
        }
    }

    public int getNumberSocketsCreated() {
        return this.objFactory.getNumberCreated();
    }

    public int getNumberSocketsDestroyed() {
        return this.objFactory.getNumberDestroyed();
    }

}
