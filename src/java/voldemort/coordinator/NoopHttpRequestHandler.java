/*
 * Copyright 2013 LinkedIn, Inc
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

import java.util.ArrayList;
import java.util.List;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;

import voldemort.common.VoldemortOpCode;
import voldemort.versioning.VectorClock;
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
        this.request = (HttpRequest) e.getMessage();
        byte operationType = getOperationType(this.request.getMethod());

        switch(operationType) {
            case VoldemortOpCode.GET_OP_CODE:
                HttpGetRequestExecutor getExecutor = new HttpGetRequestExecutor(e);

                Versioned<byte[]> responseVersioned = null;
                byte[] sampleByteArray = "a".getBytes();
                responseVersioned = new Versioned<byte[]>(sampleByteArray);
                List<Versioned<byte[]>> responseList = new ArrayList<Versioned<byte[]>>();
                responseList.add(responseVersioned);
                getExecutor.writeResponse(responseList);
                break;
            case VoldemortOpCode.PUT_OP_CODE:
                HttpPutRequestExecutor putRequestExecutor = new HttpPutRequestExecutor(e);
                putRequestExecutor.writeResponse(new VectorClock());
                break;
            default:
                System.err.println("Illegal operation.");
                return;
        }
    }

    private byte getOperationType(HttpMethod method) {
        if(method.equals(HttpMethod.POST)) {
            return VoldemortOpCode.PUT_OP_CODE;
        } else if(method.equals(HttpMethod.GET)) {
            return VoldemortOpCode.GET_OP_CODE;
        }

        return -1;
    }
}
