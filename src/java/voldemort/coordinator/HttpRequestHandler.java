package voldemort.coordinator;

/*
 * Copyright 2009 Red Hat, Inc.
 * 
 * Red Hat licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import static org.jboss.netty.handler.codec.http.HttpHeaders.isKeepAlive;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.COOKIE;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.SET_COOKIE;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
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
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.jboss.netty.util.CharsetUtil;

import voldemort.client.ZenStoreClient;
import voldemort.utils.ByteArray;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * @author <a href="http://www.jboss.org/netty/">The Netty Project</a>
 * @author Andy Taylor (andy.taylor@jboss.org)
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 * 
 * @version $Rev: 2288 $, $Date: 2010-05-27 21:40:50 +0900 (Thu, 27 May 2010) $
 */
public class HttpRequestHandler extends SimpleChannelUpstreamHandler {

    public HttpRequest request;
    private boolean readingChunks;
    /** Buffer that stores the response content */
    private final StringBuilder buf = new StringBuilder();
    private ZenStoreClient<Object, Object> storeClient;
    public ChannelBuffer responseContent;
    private final static String STORE_NAME = "store_name";

    public static enum OP_TYPE {
        GET,
        PUT
    };

    public HttpRequestHandler(ZenStoreClient<Object, Object> storeClient) {
        this.storeClient = storeClient;
    }

    public OP_TYPE getOperationType(String path) {
        if(path.equals("/put")) {
            return OP_TYPE.PUT;
        }

        return OP_TYPE.GET;
    }

    public ByteArray readKey(ChannelBuffer content) {
        int keySize = content.readInt();
        byte[] key = new byte[keySize];
        content.readBytes(key);
        return new ByteArray(key);
    }

    public void writeResults(List<Versioned<Object>> values) throws IOException {
        responseContent.writeInt(values.size());
        for(Versioned<Object> v: values) {
            byte[] clock = ((VectorClock) v.getVersion()).toBytes();
            byte[] value = (byte[]) v.getValue();
            responseContent.writeInt(clock.length + value.length);
            responseContent.writeBytes(clock);
            responseContent.writeBytes(value);
        }
    }

    private byte[] readValue(ChannelBuffer content) {
        int valueSize = content.readInt();
        byte[] value = new byte[valueSize];
        content.readBytes(value);
        return value;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        String storeName = "";
        List<Versioned<Object>> results = new ArrayList<Versioned<Object>>();

        if(!readingChunks) {
            HttpRequest request = this.request = (HttpRequest) e.getMessage();
            QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.getUri());

            // Decode the operation type
            OP_TYPE operation = getOperationType(queryStringDecoder.getPath());

            Map<String, List<String>> params = queryStringDecoder.getParameters();
            if(params != null && params.containsKey(STORE_NAME)) {
                storeName = params.get(STORE_NAME).get(0);
            } else {
                System.err.println("Store Name missing. Critical error");
                this.responseContent = ChannelBuffers.copiedBuffer("Store Name missing. Critical error".getBytes());
                return;
                // TODO: Return the right error code here
            }

            if(request.isChunked()) {
                readingChunks = true;
            } else {

                ChannelBuffer content = request.getContent();
                if(!content.readable()) {
                    System.err.println("Contents not readable");
                    this.responseContent = ChannelBuffers.copiedBuffer("Contents not readable".getBytes());
                    return;
                }

                // TODO: Check for correct number of parameters and Decoding

                switch(operation) {
                    case GET:
                        // System.out.println("GET operation");
                        ByteArray getKey = readKey(content);
                        Versioned<Object> responseVersioned = this.storeClient.get(getKey);
                        if(responseVersioned == null) {
                            byte[] nullByteArray = new byte[1];
                            nullByteArray[0] = 0;
                            responseVersioned = new Versioned<Object>(nullByteArray);
                        }
                        results.add(responseVersioned);
                        byte[] responseValue = (byte[]) responseVersioned.getValue();
                        this.responseContent = ChannelBuffers.dynamicBuffer(responseValue.length);
                        writeResults(results);
                        break;
                    case PUT:
                        // System.out.println("PUT operation");
                        ByteArray putKey = readKey(content);
                        byte[] putValue = readValue(content);
                        try {
                            Version putVersion = this.storeClient.put(putKey, putValue);
                        } catch(ObsoleteVersionException oe) {
                            // Ideally propagate the exception !
                        }
                        this.responseContent = ChannelBuffers.EMPTY_BUFFER;
                        break;
                    default:
                        System.err.println("Illegal operation.");
                        this.responseContent = ChannelBuffers.copiedBuffer("Illegal operation.".getBytes());
                        return;
                }

                writeResponse(e);
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

                writeResponse(e);
            } else {
                buf.append("CHUNK: " + chunk.getContent().toString(CharsetUtil.UTF_8) + "\r\n");
            }

        }
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
