package voldemort.coordinator;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;

import voldemort.versioning.Versioned;

public class NoopHttpRequestHandler extends VoldemortHttpRequestHandler {

    public NoopHttpRequestHandler() {}

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        OP_TYPE operation = getOperationType(this.request.getMethod());

        switch(operation) {
            case GET:
                GetRequestExecutor getExecutor = new GetRequestExecutor(null, null, e, null);

                Versioned<Object> responseVersioned = null;
                byte[] nullByteArray = new byte[1];
                nullByteArray[0] = 0;
                responseVersioned = new Versioned<Object>(nullByteArray);

                getExecutor.setResponseContent(responseVersioned);
                getExecutor.writeResponse(responseVersioned);
                break;
            case PUT:
                this.responseContent = ChannelBuffers.EMPTY_BUFFER;
                break;
            default:
                System.err.println("Illegal operation.");
                this.responseContent = ChannelBuffers.copiedBuffer("Illegal operation.".getBytes());
                return;
        }
    }
}
