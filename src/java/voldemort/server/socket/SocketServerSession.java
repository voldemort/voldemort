package voldemort.server.socket;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.Logger;

import voldemort.client.protocol.RequestFormatType;
import voldemort.server.protocol.RequestHandler;
import voldemort.server.protocol.RequestHandlerFactory;
import voldemort.utils.ByteUtils;

/**
 * Represents a session of interaction between the server and the client. This
 * begins with protocol negotiation and then a seriest of client requests
 * followed by server responses. The negotiation is handled by the session
 * object, which will choose an appropriate request handler to handle the actual
 * request/response.
 * 
 * @author jay
 * 
 */
public class SocketServerSession implements Runnable {

    private final Logger logger = Logger.getLogger(SocketServerSession.class);

    private final Socket socket;
    private final RequestHandlerFactory handlerFactory;

    public SocketServerSession(Socket socket, RequestHandlerFactory handlerFactory) {
        this.socket = socket;
        this.handlerFactory = handlerFactory;
    }

    public Socket getSocket() {
        return socket;
    }

    private boolean isInterrupted() {
        return Thread.currentThread().isInterrupted();
    }

    public void run() {
        try {
            DataInputStream inputStream = new DataInputStream(new BufferedInputStream(socket.getInputStream(),
                                                                                      64000));
            DataOutputStream outputStream = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(),
                                                                                          64000));

            RequestFormatType protocol = negotiateProtocol(inputStream, outputStream);
            RequestHandler handler = handlerFactory.getRequestHandler(protocol);
            logger.info("Client " + socket.getRemoteSocketAddress()
                        + " connected successfully with protocol " + protocol.getCode());

            while(!isInterrupted()) {
                handler.handleRequest(inputStream, outputStream);
                outputStream.flush();
            }
        } catch(EOFException e) {
            logger.info("Client " + socket.getRemoteSocketAddress() + " disconnected.");
        } catch(IOException e) {
            logger.error(e);
        } finally {
            try {
                socket.close();
            } catch(Exception e) {
                logger.error("Error while closing socket", e);
            }
        }
    }

    private RequestFormatType negotiateProtocol(InputStream input, OutputStream output)
            throws IOException {
        input.mark(3);
        byte[] protoBytes = new byte[3];
        ByteUtils.read(input, protoBytes);
        RequestFormatType requestFormat;
        try {
            String proto = ByteUtils.getString(protoBytes, "UTF-8");
            requestFormat = RequestFormatType.fromCode(proto);
            output.write(ByteUtils.getBytes("ok", "UTF-8"));
            output.flush();
        } catch(IllegalArgumentException e) {
            // okay we got some nonsense. For backwards compatibility,
            // assume this is an old client who does not know how to negotiate
            requestFormat = RequestFormatType.VOLDEMORT_V0;
            // reset input stream so we don't interfere with request format
            input.reset();
            logger.info("No protocol proposal given, assuming "
                        + RequestFormatType.VOLDEMORT_V0.getDisplayName());
        }
        return requestFormat;
    }
}