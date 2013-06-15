package voldemort.server.rest;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponse;

public class PutResponseSender extends RestResponseSender {

    public PutResponseSender(MessageEvent messageEvent) {
        super(messageEvent);
    }

    @Override
    public void sendResponse() {
        // Create the Response object
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, CREATED);

        // Set the right headers
        response.setHeader(CONTENT_LENGTH, 0);

        // Write the response to the Netty Channel
        this.messageEvent.getChannel().write(response);
    }
}
