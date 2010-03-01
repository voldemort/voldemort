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
import java.util.Map;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormatType;
import voldemort.server.protocol.RequestHandler;
import voldemort.server.protocol.RequestHandlerFactory;
import voldemort.server.protocol.StreamRequestHandler;
import voldemort.server.protocol.StreamRequestHandler.StreamRequestHandlerState;
import voldemort.utils.ByteUtils;

/**
 * Represents a session of interaction between the server and the client. This
 * begins with protocol negotiation and then a seriest of client requests
 * followed by server responses. The negotiation is handled by the session
 * object, which will choose an appropriate request handler to handle the actual
 * request/response.
 * 
 * 
 */
public class SocketServerSession implements Runnable {

    private final Logger logger = Logger.getLogger(SocketServerSession.class);

    private final Map<Long, SocketServerSession> activeSessions;
    private final long sessionId;
    private final Socket socket;
    private final RequestHandlerFactory handlerFactory;
    private volatile boolean isClosed = false;

    public SocketServerSession(Map<Long, SocketServerSession> activeSessions,
                               Socket socket,
                               RequestHandlerFactory handlerFactory,
                               long id) {
        this.activeSessions = activeSessions;
        this.socket = socket;
        this.handlerFactory = handlerFactory;
        this.sessionId = id;
    }

    public Socket getSocket() {
        return socket;
    }

    private boolean isInterrupted() {
        return Thread.currentThread().isInterrupted();
    }

    public void run() {
        try {
            activeSessions.put(sessionId, this);
            DataInputStream inputStream = new DataInputStream(new BufferedInputStream(socket.getInputStream(),
                                                                                      64000));
            DataOutputStream outputStream = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(),
                                                                                          64000));

            RequestFormatType protocol = negotiateProtocol(inputStream, outputStream);
            RequestHandler handler = handlerFactory.getRequestHandler(protocol);
            logger.info("Client " + socket.getRemoteSocketAddress()
                        + " connected successfully with protocol " + protocol.getCode());

            while(!isInterrupted() && !socket.isClosed() && !isClosed) {
                StreamRequestHandler srh = handler.handleRequest(inputStream, outputStream);

                if(srh != null) {
                    if(logger.isTraceEnabled())
                        logger.trace("Request is streaming");

                    StreamRequestHandlerState srhs = null;

                    try {
                        do {
                            if(logger.isTraceEnabled())
                                logger.trace("About to enter streaming request handler");

                            srhs = srh.handleRequest(inputStream, outputStream);

                            if(logger.isTraceEnabled())
                                logger.trace("Finished invocation of streaming request handler, result is "
                                             + srhs);

                        } while(srhs != StreamRequestHandlerState.COMPLETE);
                    } catch(VoldemortException e) {
                        srh.handleError(outputStream, e);
                        outputStream.flush();

                        break;
                    } finally {
                        srh.close(outputStream);
                    }
                }

                outputStream.flush();
            }
            if(isInterrupted())
                logger.info(Thread.currentThread().getName()
                            + " has been interrupted, closing session.");
        } catch(EOFException e) {
            logger.info("Client " + socket.getRemoteSocketAddress() + " disconnected.");
        } catch(IOException e) {
            // if this is an unexpected
            if(!isClosed)
                logger.error(e);
        } finally {
            try {
                if(!socket.isClosed())
                    socket.close();
            } catch(Exception e) {
                logger.error("Error while closing socket", e);
            }
            // now remove ourselves from the set of active sessions
            this.activeSessions.remove(sessionId);
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

    public void close() throws IOException {
        this.isClosed = true;
        this.socket.close();
    }
}
