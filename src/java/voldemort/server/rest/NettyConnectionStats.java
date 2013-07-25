package voldemort.server.rest;

import java.util.concurrent.atomic.AtomicLong;

import voldemort.annotations.jmx.JmxGetter;

public class NettyConnectionStats {

    AtomicLong activeNumberOfConnections;

    public NettyConnectionStats() {
        this.activeNumberOfConnections = new AtomicLong(0);
    }

    @JmxGetter(name = "GetActiveConnections", description = "The total nunber of active channels open and connected")
    public AtomicLong getActiveNumberOfConnections() {
        return activeNumberOfConnections;
    }

    public void reportChannelConnect() {
        this.activeNumberOfConnections.incrementAndGet();
    }

    public void reportChannelDisconnet() {
        this.activeNumberOfConnections.decrementAndGet();
    }

}
