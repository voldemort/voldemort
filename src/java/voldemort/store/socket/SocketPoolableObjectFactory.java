package voldemort.store.socket;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.apache.log4j.Logger;

/**
 * A Factory for creating sockets
 * 
 * @author jay
 * 
 */
public class SocketPoolableObjectFactory implements KeyedPoolableObjectFactory {

    public static final Logger logger = Logger.getLogger(SocketPoolableObjectFactory.class);

    private final int timeoutMs;
    public final AtomicInteger created;
    public final AtomicInteger destroyed;

    public SocketPoolableObjectFactory(int timeoutMs) {
        this.timeoutMs = timeoutMs;
        this.created = new AtomicInteger(0);
        this.destroyed = new AtomicInteger(0);
    }

    public void activateObject(Object key, Object value) throws Exception {
    // nothing to see here
    }

    public void passivateObject(Object key, Object value) throws Exception {
    // nothing to see here
    }

    public void destroyObject(Object key, Object value) throws Exception {
        SocketDestination dest = (SocketDestination) key;
        SocketAndStreams sands = (SocketAndStreams) value;
        sands.getSocket().close();
        int numDestroyed = destroyed.incrementAndGet();
        if (logger.isDebugEnabled())
            logger.debug("Destroyed socket " + numDestroyed + " connection to " + dest.getHost()
                         + ":" + dest.getPort());
    }

    public Object makeObject(Object key) throws Exception {
        SocketDestination dest = (SocketDestination) key;
        Socket s = new Socket();
        s.setReceiveBufferSize(3000);
        s.setSendBufferSize(3000);
        s.setTcpNoDelay(true);
        s.setSoTimeout(timeoutMs);
        s.connect(new InetSocketAddress(dest.getHost(), dest.getPort()));
        int numCreated = created.incrementAndGet();
        if (logger.isDebugEnabled())
            logger.debug("Created socket " + numCreated + " for " + dest.getHost() + ":"
                         + dest.getPort());
        return new SocketAndStreams(s);
    }

    public boolean validateObject(Object key, Object value) {
        SocketAndStreams sands = (SocketAndStreams) value;
        Socket s = sands.getSocket();
        return !s.isClosed() && s.isBound() && s.isConnected();
    }

    public int getTimeout() {
        return this.timeoutMs;
    }

    public int getNumberCreated() {
        return this.created.get();
    }

    public int getNumberDestroyed() {
        return this.destroyed.get();
    }

}
