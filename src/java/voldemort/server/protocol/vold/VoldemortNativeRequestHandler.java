package voldemort.server.protocol.vold;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.common.VoldemortOpCode;
import voldemort.common.nio.ByteBufferBackedInputStream;
import voldemort.common.nio.ByteBufferContainer;
import voldemort.server.RequestRoutingType;
import voldemort.server.StoreRepository;
import voldemort.server.protocol.AbstractRequestHandler;
import voldemort.server.protocol.RequestHandler;
import voldemort.server.protocol.StreamRequestHandler;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.versioning.ObsoleteVersionException;

/**
 * Server-side request handler for voldemort native client protocol
 * 
 * 
 */
public class VoldemortNativeRequestHandler extends AbstractRequestHandler implements RequestHandler {

    private static final Logger logger = Logger.getLogger(VoldemortNativeRequestHandler.class);

    private final int protocolVersion;

    public VoldemortNativeRequestHandler(ErrorCodeMapper errorMapper,
                                         StoreRepository repository,
                                         int protocolVersion) {
        super(errorMapper, repository);
        if(protocolVersion < 0 || protocolVersion > 3)
            throw new IllegalArgumentException("Unknown protocol version: " + protocolVersion);
        this.protocolVersion = protocolVersion;
    }

    private ClientRequestHandler getClientRequestHanlder(byte opCode,
                                                         Store<ByteArray, byte[], byte[]> store)
            throws IOException {
        switch(opCode) {
            case VoldemortOpCode.GET_OP_CODE:
                return new GetRequestHandler(store, protocolVersion);
            case VoldemortOpCode.GET_ALL_OP_CODE:
                return new GetAllRequestHandler(store, protocolVersion);
            case VoldemortOpCode.PUT_OP_CODE:
                return new PutRequestHandler(store, protocolVersion);
            case VoldemortOpCode.DELETE_OP_CODE:
                return new DeleteRequestHandler(store, protocolVersion);
            case VoldemortOpCode.GET_VERSION_OP_CODE:
                return new GetVersionRequestHandler(store, protocolVersion);
            default:
                throw new IOException("Unknown op code: " + opCode);
        }

    }

    @Override
    public StreamRequestHandler handleRequest(final DataInputStream inputStream,
                                              final DataOutputStream outputStream)
            throws IOException {
        return handleRequest(inputStream, outputStream, null);
    }

    private void clearBuffer(ByteBufferContainer outputContainer) {
        if(outputContainer != null) {
            outputContainer.getBuffer().clear();
        }
    }

    @Override
    public StreamRequestHandler handleRequest(final DataInputStream inputStream,
                                              final DataOutputStream outputStream,
                                              final ByteBufferContainer outputContainer)
            throws IOException {

        long startTimeMs = -1;
        long startTimeNs = -1;

        if(logger.isDebugEnabled()) {
            startTimeMs = System.currentTimeMillis();
            startTimeNs = System.nanoTime();
        }

        byte opCode = inputStream.readByte();
        String storeName = inputStream.readUTF();
        RequestRoutingType routingType = getRoutingType(inputStream);

        Store<ByteArray, byte[], byte[]> store = getStore(storeName, routingType);
        if(store == null) {
            clearBuffer(outputContainer);
            writeException(outputStream, new VoldemortException("No store named '" + storeName
                                                                + "'."));
            return null;
        }

        ClientRequestHandler requestHandler = getClientRequestHanlder(opCode, store);

        try {
            requestHandler.parseRequest(inputStream);
            requestHandler.processRequest();
        } catch ( VoldemortException e) {
            // Put generates lot of ObsoleteVersionExceptions, suppress them
            // they are harmless and indicates normal mode of operation.
            if(!(e instanceof ObsoleteVersionException)) {
                logger.error("Store" + storeName + ". Error: " + e.getMessage());
            }
            clearBuffer(outputContainer);
            writeException(outputStream, e);
            return null;
        }

        // We are done with Input, clear the buffers
        clearBuffer(outputContainer);

        int size = requestHandler.getResponseSize();
        if(outputContainer != null) {
            outputContainer.growBuffer(size);
        }

        requestHandler.writeResponse(outputStream);
        outputStream.flush();
        if(logger.isDebugEnabled()) {
            String debugPrefix = "OpCode " + opCode + " started at: " + startTimeMs
                                 + " handlerRef: " + System.identityHashCode(inputStream)
                                 + " Elapsed : " + (System.nanoTime() - startTimeNs) + " ns, ";
            
            logger.debug(debugPrefix + requestHandler.getDebugMessage() );
        }
        return null;
    }


    private RequestRoutingType getRoutingType(DataInputStream inputStream) throws IOException {
        RequestRoutingType routingType = RequestRoutingType.NORMAL;

        if(protocolVersion > 0) {
            boolean isRouted = inputStream.readBoolean();
            routingType = RequestRoutingType.getRequestRoutingType(isRouted, false);
        }

        if(protocolVersion > 1) {
            int routingTypeCode = inputStream.readByte();
            routingType = RequestRoutingType.getRequestRoutingType(routingTypeCode);
        }

        return routingType;
    }

    /**
     * This is pretty ugly. We end up mimicking the request logic here, so this
     * needs to stay in sync with handleRequest.
     */

    @Override
    public boolean isCompleteRequest(final ByteBuffer buffer) throws VoldemortException {
        DataInputStream inputStream = new DataInputStream(new ByteBufferBackedInputStream(buffer));

        try {

            byte opCode = inputStream.readByte();
            // Store Name
            inputStream.readUTF();
            // Store routing type
            getRoutingType(inputStream);

            switch(opCode) {
                case VoldemortOpCode.GET_VERSION_OP_CODE:
                    if(!GetVersionRequestHandler.isCompleteRequest(inputStream, buffer))
                        return false;
                    break;
                case VoldemortOpCode.GET_OP_CODE:
                    if(!GetRequestHandler.isCompleteRequest(inputStream, buffer, protocolVersion))
                        return false;
                    break;
                case VoldemortOpCode.GET_ALL_OP_CODE:
                    if(!GetAllRequestHandler.isCompleteRequest(inputStream, buffer, protocolVersion))
                        return false;
                    break;
                case VoldemortOpCode.PUT_OP_CODE: {
                    if(!PutRequestHandler.isCompleteRequest(inputStream, buffer, protocolVersion))
                        return false;
                    break;
                }
                case VoldemortOpCode.DELETE_OP_CODE: {
                    if(!DeleteRequestHandler.isCompleteRequest(inputStream, buffer))
                        return false;
                    break;
                }
                default:
                    throw new VoldemortException(" Unrecognized Voldemort OpCode " + opCode);
            }
            // This should not happen, if we reach here and if buffer has more
            // data, there is something wrong.
            if(buffer.hasRemaining()) {
                logger.info(" Probably a client bug, Discarding additional bytes in isCompleteRequest. Opcode "
                            + opCode
                            + " remaining bytes " + buffer.remaining());
            }
            return true;
        } catch(IOException e) {
            // This could also occur if the various methods we call into
            // re-throw a corrupted value error as some other type of exception.
            // For example, updating the position on a buffer past its limit
            // throws an InvalidArgumentException.
            if(logger.isDebugEnabled())
                logger.debug("Probable partial read occurred causing exception", e);

            return false;
        }
    }

    private void writeException(DataOutputStream stream, VoldemortException e) throws IOException {
        short code = getErrorMapper().getCode(e);
        stream.writeShort(code);
        stream.writeUTF(e.getMessage());
        stream.flush();
    }

}
