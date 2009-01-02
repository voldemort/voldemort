package voldemort.server.socket;

import java.util.concurrent.ConcurrentMap;

import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxManaged;
import voldemort.server.AbstractService;
import voldemort.server.VoldemortService;
import voldemort.store.Store;

/**
 * The VoldemortService that loads up the socket server
 * 
 * @author jay
 * 
 */
@JmxManaged(description = "A server that handles remote operations on stores via tcp/ip.")
public class SocketService extends AbstractService implements VoldemortService {

    private final SocketServer server;

    public SocketService(String name,
                         ConcurrentMap<String, ? extends Store<byte[], byte[]>> storeMap,
                         int port,
                         int coreConnections,
                         int maxConnections) {
        super(name);
        this.server = new SocketServer(storeMap, port, coreConnections, maxConnections);
    }

    @Override
    protected void startInner() {
        this.server.start();
    }

    @Override
    protected void stopInner() {
        this.server.shutdown();
    }

    @JmxGetter(name = "port", description = "The port on which the server is accepting connections.")
    public int getPort() {
        return server.getPort();
    }

}
