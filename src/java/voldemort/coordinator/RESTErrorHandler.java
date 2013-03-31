/*
 * Copyright 2008-2013 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.coordinator;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.util.CharsetUtil;

/**
 * A Generic class used to propagate the error back to the client over the Netty
 * channel
 * 
 */
public class RESTErrorHandler {

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
}
