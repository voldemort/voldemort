package voldemort.coordinator;

import static org.jboss.netty.handler.codec.http.HttpHeaders.isKeepAlive;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.COOKIE;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.SET_COOKIE;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.Cookie;
import org.jboss.netty.handler.codec.http.CookieDecoder;
import org.jboss.netty.handler.codec.http.CookieEncoder;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpChunkTrailer;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.util.CharsetUtil;

import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public class VoldemortHttpRequestHandler extends SimpleChannelUpstreamHandler {

    public HttpRequest request;
    private boolean readingChunks;
    /** Buffer that stores the response content */
    private final StringBuilder buf = new StringBuilder();
    public ChannelBuffer responseContent;
    private Map<String, FatClientWrapper> fatClientMap;
    private final Logger logger = Logger.getLogger(VoldemortHttpRequestHandler.class);

    public static enum OP_TYPE {
        GET,
        PUT
    }

    // Implicit constructor defined for the derived classes
    public VoldemortHttpRequestHandler() {}

    public VoldemortHttpRequestHandler(Map<String, FatClientWrapper> fatClientMap) {
        this.fatClientMap = fatClientMap;
    }

    public OP_TYPE getOperationType(HttpMethod httpMethod) {
        if(httpMethod.equals(HttpMethod.PUT)) {
            return OP_TYPE.PUT;
        }

        return OP_TYPE.GET;
    }

    public void writeResults(List<Versioned<Object>> values) {
        responseContent.writeInt(values.size());
        for(Versioned<Object> v: values) {
            byte[] clock = ((VectorClock) v.getVersion()).toBytes();
            byte[] value = (byte[]) v.getValue();
            responseContent.writeInt(clock.length + value.length);
            responseContent.writeBytes(clock);
            responseContent.writeBytes(value);
        }
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {

        String storeName = "";

        if(!readingChunks) {
            HttpRequest request = this.request = (HttpRequest) e.getMessage();
            OP_TYPE operation = getOperationType(this.request.getMethod());
            String requestURI = this.request.getUri();
            logger.info(requestURI);

            storeName = getStoreName(requestURI);
            if(storeName == null) {
                String errorMessage = "Invalid store name. Critical error.";
                // this.responseContent =
                // ChannelBuffers.copiedBuffer("Invalid store name. Critical error.".getBytes());
                logger.error(errorMessage);
                RESTErrorHandler.handleError(BAD_REQUEST, e, false, errorMessage);
                return;
            }

            if(request.isChunked()) {
                readingChunks = true;
            } else {

                // TODO: Check for correct number of parameters and Decoding

                switch(operation) {
                    case GET:
                        ByteArray getKey = readKey(requestURI);
                        this.fatClientMap.get(storeName).submitGetRequest(getKey, e);
                        break;
                    case PUT:
                        ChannelBuffer content = request.getContent();
                        if(!content.readable()) {
                            String errorMessage = "Contents not readable";
                            // this.responseContent =
                            // ChannelBuffers.copiedBuffer("Contents not readable".getBytes());
                            logger.error(errorMessage);
                            RESTErrorHandler.handleError(BAD_REQUEST,
                                                         e,
                                                         isKeepAlive(request),
                                                         errorMessage);
                            return;
                        }

                        ByteArray putKey = readKey(requestURI);
                        byte[] putValue = readValue(content);
                        this.fatClientMap.get(storeName).submitPutRequest(putKey, putValue, e);
                        break;
                    default:
                        String errorMessage = "Illegal operation.";
                        logger.error(errorMessage);
                        RESTErrorHandler.handleError(BAD_REQUEST,
                                                     e,
                                                     isKeepAlive(request),
                                                     errorMessage);
                        return;
                }

            }
        } else {
            HttpChunk chunk = (HttpChunk) e.getMessage();
            if(chunk.isLast()) {
                readingChunks = false;
                buf.append("END OF CONTENT\r\n");

                HttpChunkTrailer trailer = (HttpChunkTrailer) chunk;
                if(!trailer.getHeaderNames().isEmpty()) {
                    buf.append("\r\n");
                    for(String name: trailer.getHeaderNames()) {
                        for(String value: trailer.getHeaders(name)) {
                            buf.append("TRAILING HEADER: " + name + " = " + value + "\r\n");
                        }
                    }
                    buf.append("\r\n");
                }

            } else {
                buf.append("CHUNK: " + chunk.getContent().toString(CharsetUtil.UTF_8) + "\r\n");
            }

        }
    }

    private byte[] readValue(ChannelBuffer content) {
        byte[] value = new byte[content.capacity()];
        content.readBytes(value);
        return value;
    }

    private ByteArray readKey(String requestURI) {
        ByteArray key = null;
        String[] parts = requestURI.split("/");
        if(parts.length > 2) {
            String base64Key = parts[2];
            key = new ByteArray(Base64.decodeBase64(base64Key.getBytes()));
        }
        return key;
    }

    private String getStoreName(String requestURI) {
        String storeName = null;
        String[] parts = requestURI.split("/");
        if(parts.length > 1 && this.fatClientMap.containsKey(parts[1])) {
            storeName = parts[1];
        }

        return storeName;
    }

    public void writeResponse(MessageEvent e) {
        // Decide whether to close the connection or not.
        boolean keepAlive = isKeepAlive(request);

        // Build the response object.
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        response.setContent(this.responseContent);
        // response.setHeader(CONTENT_TYPE, "text/plain; charset=UTF-8");
        response.setHeader(CONTENT_TYPE, "application/pdf");
        // response.setChunked(true);

        if(keepAlive) {
            // Add 'Content-Length' header only for a keep-alive connection.
            response.setHeader(CONTENT_LENGTH, response.getContent().readableBytes());
        }

        // Encode the cookie.
        String cookieString = request.getHeader(COOKIE);
        if(cookieString != null) {
            CookieDecoder cookieDecoder = new CookieDecoder();
            Set<Cookie> cookies = cookieDecoder.decode(cookieString);
            if(!cookies.isEmpty()) {
                // Reset the cookies if necessary.
                CookieEncoder cookieEncoder = new CookieEncoder(true);
                for(Cookie cookie: cookies) {
                    cookieEncoder.addCookie(cookie);
                }
                response.addHeader(SET_COOKIE, cookieEncoder.encode());
            }
        }

        // Write the response.
        ChannelFuture future = e.getChannel().write(response);

        // Close the non-keep-alive connection after the write operation is
        // done.
        if(!keepAlive) {
            future.addListener(ChannelFutureListener.CLOSE);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        e.getCause().printStackTrace();
        e.getChannel().close();
    }
}
