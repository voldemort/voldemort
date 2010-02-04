package voldemort.server.socket;

import voldemort.server.protocol.RequestHandlerFactory;

@Deprecated
public class AdminService extends SocketService {

    public AdminService(RequestHandlerFactory requestHandlerFactory,
                        int port,
                        int coreConnections,
                        int maxConnections,
                        int socketBufferSize,
                        String serviceName,
                        boolean enableJmx) {
        super(requestHandlerFactory,
              port,
              coreConnections,
              maxConnections,
              socketBufferSize,
              serviceName,
              enableJmx);
    }

}
