package voldemort.server.http;

import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.thread.BoundedThreadPool;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxManaged;
import voldemort.server.AbstractService;
import voldemort.server.VoldemortServer;
import voldemort.server.http.gui.AdminServlet;
import voldemort.server.http.gui.VelocityEngine;

/**
 * An embedded http server that uses jetty
 * 
 * @author jay
 * 
 */
@JmxManaged(description = "A store connector that serves remote clients via HTTP.")
public class HttpService extends AbstractService {

    private final int port;
    private final int numberOfThreads;
    private final VoldemortServer server;
    private final VelocityEngine velocityEngine;
    private Server httpServer;
    private Context context;

    public HttpService(String name, VoldemortServer server, int numberOfThreads, int httpPort) {
        super(name);
        this.port = httpPort;
        this.numberOfThreads = numberOfThreads;
        this.server = server;
        this.velocityEngine = new VelocityEngine(VoldemortServletContextListener.VOLDEMORT_TEMPLATE_DIR);
    }

    @Override
    public void startInner() {
        try {
            Connector connector = new SelectChannelConnector();
            connector.setLowResourceMaxIdleTime(3000);
            connector.setPort(this.port);
            BoundedThreadPool threadPool = new BoundedThreadPool();
            threadPool.setName("VoldemortHttp");
            threadPool.setMaxThreads(this.numberOfThreads);
            Server httpServer = new Server();
            httpServer.setConnectors(new Connector[] { connector });
            httpServer.setThreadPool(threadPool);
            httpServer.setSendServerVersion(false);
            httpServer.setSendDateHeader(false);
            Context context = new Context(httpServer, "/", Context.NO_SESSIONS);
            context.setAttribute(VoldemortServletContextListener.SERVER_CONFIG_KEY, server);
            context.addServlet(new ServletHolder(new AdminServlet(server, velocityEngine)),
                               "/admin");
            context.addServlet(new ServletHolder(new StoreServlet(server.getStoreMap())), "/*");
            this.context = context;
            this.httpServer = httpServer;
            this.httpServer.start();
        } catch (Exception e) {
            throw new VoldemortException(e);
        }
    }

    @Override
    public void stopInner() {
        try {
            httpServer.stop();
            context.destroy();
        } catch (Exception e) {
            throw new VoldemortException(e);
        }
        this.httpServer = null;
        this.context = null;
    }

    @JmxGetter(name = "numberOfThreads", description = "The number of threads used for the thread pool for HTTP.")
    public int getNumberOfThreads() {
        return numberOfThreads;
    }

    @JmxGetter(name = "port", description = "The port on which http connections are accepted.")
    public int getPort() {
        return port;
    }

}
