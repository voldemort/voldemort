package voldemort.store.socket;

import com.google.common.base.Objects;

/**
 * A host + port
 * 
 * @author jay
 * 
 */
public class SocketDestination {

    private final String host;
    private final int port;

    public SocketDestination(String host, int port) {
        this.host = Objects.nonNull(host);
        this.port = Objects.nonNull(port);
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;

        if (obj == null || !obj.getClass().equals(SocketDestination.class))
            return false;

        SocketDestination d = (SocketDestination) obj;
        return getHost().equals(d.getHost()) && getPort() == d.getPort();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(host, port);
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }

}
