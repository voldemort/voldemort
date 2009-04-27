package voldemort.server.socket;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;

import voldemort.server.protocol.RequestHandler;

public class SocketServerSession implements Runnable {

    private final Socket socket;
    private final RequestHandler requestHandler;

    public SocketServerSession(Socket socket, RequestHandler requestHandler) {
        this.socket = socket;
        this.requestHandler = requestHandler;
    }

    public Socket getSocket() {
        return socket;
    }

    private boolean isInterrupted() {
        return Thread.currentThread().isInterrupted();
    }

    public void run() {
        try {
            SocketServer.logger.info("Client " + socket.getRemoteSocketAddress() + " connected.");
            DataInputStream inputStream = new DataInputStream(new BufferedInputStream(socket.getInputStream(),
                                                                                      64000));
            DataOutputStream outputStream = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(),
                                                                                          64000));
            while(!isInterrupted()) {
                requestHandler.handleRequest(inputStream, outputStream);
                outputStream.flush();
            }
        } catch(EOFException e) {
            SocketServer.logger.info("Client " + socket.getRemoteSocketAddress() + " disconnected.");
        } catch(IOException e) {
            SocketServer.logger.error(e);
        } finally {
            try {
                socket.close();
            } catch(Exception e) {
                SocketServer.logger.error("Error while closing socket", e);
            }
        }
    }
}