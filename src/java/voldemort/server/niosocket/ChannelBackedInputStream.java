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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.IllegalBlockingModeException;
import java.nio.channels.ReadableByteChannel;

import voldemort.annotations.concurrency.NotThreadsafe;
import voldemort.server.protocol.RequestHandler;

/**
 * ChannelBackedInputStream is a simple wrapper around the delegated Channel
 * implementation. The need for this class is two-fold:
 * <p/>
 * <ol>
 * <li>The {@link RequestHandler} API requires an InputStream rather than a
 * Channel or ByteBuffer. In order to keep that API as it is, we wrap the
 * NIO-based implementation in a "classic" InputStream.</li>
 * <li>The Channels.newInputStream API clearly states that the returned
 * InputStream will throw an {@link IllegalBlockingModeException} if the
 * underlying channel is in non-blocking mode. Since we <i>are</i> in
 * non-blocking mode, using the implementation provided by newInputStream is
 * guaranteed not to work.</li>
 * </ol>
 * 
 * @author Kirk True
 * 
 * @see voldemort.server.protocol.RequestHandler
 * @see voldemort.server.niosocket.ChannelBackedOutputStream
 */

@NotThreadsafe
public class ChannelBackedInputStream extends InputStream {

    private final ReadableByteChannel channel;

    private final ByteBuffer singleByteBuffer;

    public ChannelBackedInputStream(ReadableByteChannel readableByteChannel) {
        this.channel = readableByteChannel;
        this.singleByteBuffer = ByteBuffer.allocate(1);
    }

    @Override
    public int read() throws IOException {
        singleByteBuffer.clear();
        int n = channel.read(singleByteBuffer);

        if(n == -1)
            return -1;
        else
            return singleByteBuffer.get(0);
    }

    /**
     * This is the implementation of the classic read API from InputStream. We
     * simply call ByteBuffer.wrap on the provided byte array and call
     * Channel.read to read in the data.
     */

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(b, off, len);
        return channel.read(buffer);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

}
