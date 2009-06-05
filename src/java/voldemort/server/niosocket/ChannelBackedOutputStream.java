/*
 * Copyright 2009 LinkedIn, Inc
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

package voldemort.server.niosocket;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.IllegalBlockingModeException;
import java.nio.channels.WritableByteChannel;

import voldemort.annotations.concurrency.NotThreadsafe;
import voldemort.server.protocol.RequestHandler;

/**
 * ChannelBackedOutputStream is a simple wrapper around the delegated Channel
 * implementation. The need for this class is two-fold:
 * <p/>
 * <ol>
 * <li>The {@link RequestHandler} API requires an OutputStream rather than a
 * Channel or ByteBuffer. In order to keep that API as it is, we wrap the
 * NIO-based implementation in a "classic" OutputStream.</li>
 * <li>The Channels.newOutputStream API clearly states that the returned
 * OutputStream will throw an {@link IllegalBlockingModeException} if the
 * underlying channel is in non-blocking mode. Since we <i>are</i> in
 * non-blocking mode, using the implementation provided by newOutputStream is
 * guaranteed not to work.</li>
 * </ol>
 * 
 * @author Kirk True
 * 
 * @see voldemort.server.protocol.RequestHandler
 * @see voldemort.server.niosocket.ChannelBackedInputStream
 */

@NotThreadsafe
// Test
public class ChannelBackedOutputStream extends OutputStream {

    private final WritableByteChannel channel;

    private final ByteBuffer singleByteBuffer;

    public ChannelBackedOutputStream(WritableByteChannel channel) {
        this.channel = channel;
        this.singleByteBuffer = ByteBuffer.allocate(1);
    }

    @Override
    public void write(int b) throws IOException {
        singleByteBuffer.clear();
        singleByteBuffer.put(0, (byte) b);
        channel.write(singleByteBuffer);
    }

    /**
     * This is the implementation of the classic write API from OutputStream. We
     * simply call ByteBuffer.wrap on the provided byte array and call
     * Channel.write to write the data.
     */

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(b, off, len);
        channel.write(buffer);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

}
