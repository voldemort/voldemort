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

package voldemort.server.socket;

import voldemort.common.service.ServiceType;
import voldemort.server.AbstractSocketService;
import voldemort.server.StatusManager;
import voldemort.server.protocol.RequestHandlerFactory;

/**
 * The VoldemortService that loads up the socket server
 * 
 */

public class SocketService extends AbstractSocketService {

    private final SocketServer server;

    public SocketService(RequestHandlerFactory requestHandlerFactory,
                         int port,
                         int coreConnections,
                         int maxConnections,
                         int socketBufferSize,
                         String serviceName,
                         boolean enableJmx) {
        super(ServiceType.SOCKET, port, serviceName, enableJmx);
        this.server = new SocketServer(port,
                                       coreConnections,
                                       maxConnections,
                                       socketBufferSize,
                                       requestHandlerFactory,
                                       serviceName);
    }

    @Override
    public StatusManager getStatusManager() {
        return server.getStatusManager();
    }

    @Override
    protected void startInner() {
        this.server.start();
        this.server.awaitStartupCompletion();
        enableJmx(server);
    }

    @Override
    protected void stopInner() {
        this.server.shutdown();
    }
}
