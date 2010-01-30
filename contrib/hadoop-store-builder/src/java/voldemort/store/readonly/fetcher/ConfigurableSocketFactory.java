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

/**
 * A lot of boilerplate code to tell hadoop to use a non-standard socket buffer
 * size. This is necessary for good performance over a higher latency link such
 * as between datacenters.
 * 
 * @author jay
 * 
 */
class ConfigurableSocketFactory extends SocketFactory implements Configurable {

    private static final Logger logger = Logger.getLogger(ConfigurableSocketFactory.class);

    static {
        logger.info("----- Using configurable socket factory -------");
    }

    private Configuration configuration;
    private int socketReceiveBufferSize = -1;

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
        socket.connect(new InetSocketAddress(localAddress, port));
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
        socket.connect(new InetSocketAddress(host, port));
        return socket;
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException, UnknownHostException {
        Socket socket = applySettings(createSocket());
        socket.connect(new InetSocketAddress(host, port));
        return socket;
    }

    public Configuration getConf() {
        return this.configuration;
    }

    public void setConf(Configuration conf) {
        this.configuration = conf;
        this.socketReceiveBufferSize = conf.getInt("io.socket.receive.buffer", 100 * 1024 * 1024);
    }

    private Socket applySettings(Socket s) throws IOException {
        if(logger.isDebugEnabled())
            logger.debug("Attempting to set socket receive buffer of "
                         + this.socketReceiveBufferSize + " bytes");
        s.setReceiveBufferSize(socketReceiveBufferSize);
        if(logger.isDebugEnabled())
            logger.info("Actually set socket receive buffer to " + s.getReceiveBufferSize()
                        + " bytes");
        return s;
    }

}