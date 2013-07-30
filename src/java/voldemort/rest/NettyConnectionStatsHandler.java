package voldemort.rest;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

/**
 * Handler that monitors active connections to the Voldemort Rest Service
 * 
 */
public class NettyConnectionStatsHandler extends SimpleChannelHandler {

    private final NettyConnectionStats connectionStats;

    public NettyConnectionStatsHandler(NettyConnectionStats connectionStats) {
        this.connectionStats = connectionStats;
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        connectionStats.reportChannelConnect();
        ctx.sendUpstream(e);
    }

    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e)
            throws Exception {
        connectionStats.reportChannelDisconnet();
        ctx.sendUpstream(e);
    }

}
