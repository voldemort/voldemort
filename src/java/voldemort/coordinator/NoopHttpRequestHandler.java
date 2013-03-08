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

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;

import voldemort.common.VoldemortOpCode;
import voldemort.store.VoldemortRequestWrapper;
import voldemort.versioning.Versioned;

/**
 * A class that does a Noop after handling a REST request from the thin client.
 * This is used for benchmarking purposes.
 * 
 * 
 */
public class NoopHttpRequestHandler extends VoldemortHttpRequestHandler {

    public NoopHttpRequestHandler() {}

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        byte operationType = getOperationType(this.request.getMethod());

        switch(operationType) {
            case VoldemortOpCode.GET_OP_CODE:
                GetRequestExecutor getExecutor = new GetRequestExecutor(new VoldemortRequestWrapper(null,
                                                                                                    0l,
                                                                                                    false),
                                                                        e,
                                                                        null);

                Versioned<Object> responseVersioned = null;
                byte[] nullByteArray = new byte[1];
                nullByteArray[0] = 0;
                responseVersioned = new Versioned<Object>(nullByteArray);
                getExecutor.writeResponse(responseVersioned);
                break;
            case VoldemortOpCode.PUT_OP_CODE:
                this.responseContent = ChannelBuffers.EMPTY_BUFFER;
                break;
            default:
                System.err.println("Illegal operation.");
                this.responseContent = ChannelBuffers.copiedBuffer("Illegal operation.".getBytes());
                return;
        }
    }
}
