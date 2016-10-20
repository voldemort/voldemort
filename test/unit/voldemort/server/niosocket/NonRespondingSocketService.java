package voldemort.server.niosocket;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;


public class NonRespondingSocketService {

    private final ServerSocket serverSocket;
    public NonRespondingSocketService(int port) throws IOException {
        // server socket with single element backlog queue (1) and dynamically
        // allocated port (0)
        serverSocket = new ServerSocket(port, 1);
        // just get the allocated port
        port = serverSocket.getLocalPort();
        // fill backlog queue by this request so consequent requests will be
        // blocked
        new Socket().connect(serverSocket.getLocalSocketAddress());

    }

    public void start() {

    }

    public void stop() throws IOException {
        if(serverSocket != null && !serverSocket.isClosed()) {
            serverSocket.close();
        }
    }
}
