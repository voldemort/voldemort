package voldemort.coordinator;

import java.util.ArrayList;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;

import voldemort.versioning.Versioned;

public class NoopHttpRequestHandler extends HttpRequestHandler {

    public NoopHttpRequestHandler() {}

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        List<Versioned<Object>> results = new ArrayList<Versioned<Object>>();

        HttpRequest request = this.request = (HttpRequest) e.getMessage();
        QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.getUri());

        // Decode the operation type
        OP_TYPE operation = getOperationType(queryStringDecoder.getPath());

        switch(operation) {
            case GET:
                Versioned<Object> responseVersioned = null;
                byte[] nullByteArray = new byte[1];
                nullByteArray[0] = 0;
                responseVersioned = new Versioned<Object>(nullByteArray);
                results.add(responseVersioned);
                byte[] responseValue = (byte[]) responseVersioned.getValue();
                this.responseContent = ChannelBuffers.dynamicBuffer(responseValue.length);
                writeResults(results);
                break;
            case PUT:
                this.responseContent = ChannelBuffers.EMPTY_BUFFER;
                break;
            default:
                System.err.println("Illegal operation.");
                this.responseContent = ChannelBuffers.copiedBuffer("Illegal operation.".getBytes());
                return;
        }

        writeResponse(e);

    }
}
