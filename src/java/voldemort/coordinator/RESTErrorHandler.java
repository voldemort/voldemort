package voldemort.coordinator;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.util.CharsetUtil;

public class RESTErrorHandler {

    private static final Logger logger = Logger.getLogger(RESTErrorHandler.class);

    public static void handleError(HttpResponseStatus status,
                                   MessageEvent e,
                                   boolean keepAlive,
                                   String message) {
        // 1. Create the Response object
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, status);

        response.setHeader(CONTENT_TYPE, "text/plain; charset=UTF-8");
        response.setContent(ChannelBuffers.copiedBuffer("Failure: " + status.toString() + ". "
                                                        + message + "\r\n", CharsetUtil.UTF_8));

        // Write the response to the Netty Channel
        ChannelFuture future = e.getChannel().write(response);

        // Close the non-keep-alive connection after the write operation is
        // done.
        if(!keepAlive) {
            future.addListener(ChannelFutureListener.CLOSE);
        }
    }

    public static void handleBadRequestError(MessageEvent e, boolean keepAlive) {
        // 1. Create the Response object
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, BAD_REQUEST);

        // Write the response to the Netty Channel
        ChannelFuture future = e.getChannel().write(response);

        // Close the non-keep-alive connection after the write operation is
        // done.
        if(!keepAlive) {
            future.addListener(ChannelFutureListener.CLOSE);
        }
    }

    public static void handleInternalServerError(MessageEvent e, boolean keepAlive) {
        // 1. Create the Response object
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, INTERNAL_SERVER_ERROR);

        // Write the response to the Netty Channel
        ChannelFuture future = e.getChannel().write(response);

        // Close the non-keep-alive connection after the write operation is
        // done.
        if(!keepAlive) {
            future.addListener(ChannelFutureListener.CLOSE);
        }
    }
}
