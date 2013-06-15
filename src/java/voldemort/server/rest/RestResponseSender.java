package voldemort.server.rest;

import org.jboss.netty.channel.MessageEvent;

public abstract class RestResponseSender {

    protected MessageEvent messageEvent;

    public RestResponseSender(MessageEvent messageEvent) {
        this.messageEvent = messageEvent;
    }

    public abstract void sendResponse() throws Exception;
}
