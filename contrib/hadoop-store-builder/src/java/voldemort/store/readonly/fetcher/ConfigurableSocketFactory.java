package voldemort.store.readonly.fetcher;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;

import javax.net.SocketFactory;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import voldemort.server.VoldemortConfig;

/**
 * A lot of boilerplate code to tell hadoop to use a non-standard socket buffer
 * size. This is necessary for good performance over a higher latency link such
 * as between datacenters.
 * 
 * 
 */
public class ConfigurableSocketFactory extends SocketFactory implements Configurable {

    private static final Logger logger = Logger.getLogger(ConfigurableSocketFactory.class);
    public static final String SO_RCVBUF = "io.socket.receive.buffer";
    public static final String SO_TIMEOUT = "io.socket.timeout";

    static {
        logger.info("----- Using configurable socket factory -------");
    }

    private Configuration configuration;
    private int socketReceiveBufferSize;
    private int socketTimeout;

    @Override
    public Socket createSocket() throws IOException {
        return applySettings(SocketChannel.open().socket());
    }

    @Override
    public Socket createSocket(InetAddress address,
                               int port,
                               InetAddress localAddress,
                               int localPort) throws IOException {
        Socket socket = applySettings(createSocket());
        socket.bind(new InetSocketAddress(address, localPort));
        socket.connect(new InetSocketAddress(localAddress, port), socketTimeout);
        return socket;
    }

    @Override
    public Socket createSocket(InetAddress host, int port) throws IOException {
        Socket socket = applySettings(createSocket());
        socket.connect(new InetSocketAddress(host, port));
        return socket;
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress localHost, int localPort)
            throws IOException, UnknownHostException {
        Socket socket = applySettings(createSocket());
        socket.bind(new InetSocketAddress(host, localPort));
        socket.connect(new InetSocketAddress(host, port), socketTimeout);
        return socket;
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException, UnknownHostException {
        Socket socket = applySettings(createSocket());
        socket.connect(new InetSocketAddress(host, port), socketTimeout);
        return socket;
    }

    public Configuration getConf() {
        return this.configuration;
    }

    public void setConf(Configuration conf) {
        this.configuration = conf;
        this.socketReceiveBufferSize = conf.getInt(SO_RCVBUF, VoldemortConfig.DEFAULT_FETCHER_BUFFER_SIZE);
        this.socketTimeout = conf.getInt(SO_TIMEOUT, VoldemortConfig.DEFAULT_FETCHER_SOCKET_TIMEOUT);
    }

    private Socket applySettings(Socket s) throws IOException {
        if(logger.isDebugEnabled())
            logger.debug("Attempting to set socket receive buffer of "
                         + this.socketReceiveBufferSize + " bytes");
        s.setReceiveBufferSize(socketReceiveBufferSize);
        s.setSoTimeout(socketTimeout);
        if(logger.isDebugEnabled())
            logger.info("Actually set socket receive buffer to " + s.getReceiveBufferSize()
                        + " bytes");
        return s;
    }

}
