package voldemort.rest;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;

/**
 * Handler that monitors active connections to the Voldemort Rest Service
 * 
 */
public class NettyConnectionStatsHandler extends SimpleChannelHandler {

    private final NettyConnectionStats connectionStats;
    private final ChannelGroup allchannels;

    public NettyConnectionStatsHandler(NettyConnectionStats connectionStats,
                                       ChannelGroup allchannels) {
        this.connectionStats = connectionStats;
        this.allchannels = allchannels;
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        connectionStats.reportChannelConnect();
        if(allchannels != null) {
            allchannels.add(e.getChannel());
        }
        ctx.sendUpstream(e);
    }

    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e)
            throws Exception {
        connectionStats.reportChannelDisconnet();
        ctx.sendUpstream(e);
    }

}
