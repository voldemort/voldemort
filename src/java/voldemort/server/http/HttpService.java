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

package voldemort.server.http;

import org.apache.log4j.Logger;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.thread.QueuedThreadPool;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxManaged;
import voldemort.client.protocol.RequestFormatType;
import voldemort.common.service.AbstractService;
import voldemort.common.service.ServiceType;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortServer;
import voldemort.server.http.gui.AdminServlet;
import voldemort.server.http.gui.ReadOnlyStoreManagementServlet;
import voldemort.server.http.gui.StatusServlet;
import voldemort.server.http.gui.VelocityEngine;
import voldemort.server.protocol.RequestHandler;
import voldemort.server.protocol.SocketRequestHandlerFactory;
import voldemort.server.storage.StorageService;

/**
 * An embedded http server that uses jetty
 * 
 * 
 */
@JmxManaged(description = "A store connector that serves remote clients via HTTP.")
public class HttpService extends AbstractService {

    private final Logger logger = Logger.getLogger(HttpService.class);

    private final int port;
    private final int numberOfThreads;
    private final VoldemortServer server;
    private final VelocityEngine velocityEngine;
    private final RequestHandler requestHandler;
    private Server httpServer;
    private Context context;

    @SuppressWarnings("unused")
    public HttpService(VoldemortServer server,
                       StorageService storageService,
                       StoreRepository storeRepository,
                       RequestFormatType requestType,
                       int numberOfThreads,
                       int httpPort) {
        super(ServiceType.HTTP);
        this.port = httpPort;
        this.numberOfThreads = numberOfThreads;
        this.server = server;
        this.velocityEngine = new VelocityEngine(VoldemortServletContextListener.VOLDEMORT_TEMPLATE_DIR);
        this.requestHandler = new SocketRequestHandlerFactory(storageService,
                                                              server.getStoreRepository(),
                                                              server.getMetadataStore(),
                                                              server.getVoldemortConfig(),
                                                              server.getAsyncRunner(),
                                                              null).getRequestHandler(requestType);
    }

    @Override
    public void startInner() {
        try {
            Connector connector = new SelectChannelConnector();
            connector.setLowResourceMaxIdleTime(3000);
            connector.setPort(this.port);
            QueuedThreadPool threadPool = new QueuedThreadPool();
            threadPool.setName("VoldemortHttp");
            threadPool.setMaxThreads(this.numberOfThreads);
            Server httpServer = new Server();
            httpServer.setConnectors(new Connector[] { connector });
            httpServer.setThreadPool(threadPool);
            httpServer.setSendServerVersion(false);
            httpServer.setSendDateHeader(false);
            Context context = new Context(httpServer, "/", Context.NO_SESSIONS);
            context.setAttribute(VoldemortServletContextListener.SERVER_KEY, server);
            context.setAttribute(VoldemortServletContextListener.VELOCITY_ENGINE_KEY,
                                 velocityEngine);
            context.addServlet(new ServletHolder(new AdminServlet(server, velocityEngine)),
                               "/admin");
            context.addServlet(new ServletHolder(new StoreServlet(requestHandler)), "/stores");
            context.addServlet(new ServletHolder(new ReadOnlyStoreManagementServlet(server,
                                                                                    velocityEngine)),
                               "/read-only/mgmt");
            context.addServlet(new ServletHolder(new StatusServlet(server, velocityEngine)),
                               "/server-status");
            this.context = context;
            this.httpServer = httpServer;
            this.httpServer.start();
            logger.info("HTTP service started on port " + this.port);
        } catch(Exception e) {
            throw new VoldemortException(e);
        }
    }

    @Override
    public void stopInner() {
        try {
            if(httpServer != null)
                httpServer.stop();
            if(context != null)
                context.destroy();
        } catch(Exception e) {
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

    public RequestHandler getRequestHandler() {
        return requestHandler;
    }

}
