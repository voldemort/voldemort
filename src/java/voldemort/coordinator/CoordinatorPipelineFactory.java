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

import static org.jboss.netty.channel.Channels.pipeline;

import java.util.Map;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpContentCompressor;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;

/**
 * A PipelineFactory implementation to setup the Netty Pipeline in the
 * Coordinator
 * 
 */
public class CoordinatorPipelineFactory implements ChannelPipelineFactory {

    private boolean noop = false;
    private Map<String, FatClientWrapper> fatClientMap;

    public CoordinatorPipelineFactory(Map<String, FatClientWrapper> fatClientMap, boolean noop) {
        this.fatClientMap = fatClientMap;
        this.noop = noop;
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
        // Create a default pipeline implementation.
        ChannelPipeline pipeline = pipeline();

        pipeline.addLast("decoder", new HttpRequestDecoder());
        pipeline.addLast("aggregator", new HttpChunkAggregator(1048576));
        pipeline.addLast("encoder", new HttpResponseEncoder());
        // Remove the following line if you don't want automatic content
        // compression.
        pipeline.addLast("deflater", new HttpContentCompressor());
        if(this.noop) {
            pipeline.addLast("handler", new NoopHttpRequestHandler());
        } else {
            pipeline.addLast("handler", new VoldemortHttpRequestHandler(this.fatClientMap));
        }
        return pipeline;
    }
}
