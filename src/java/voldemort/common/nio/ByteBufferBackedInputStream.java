/*
 * Copyright 2009 Mustard Grain, Inc
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

package voldemort.common.nio;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import voldemort.annotations.concurrency.NotThreadsafe;

/**
 * ByteBufferBackedInputStream allows a ByteBuffer to be the source of data for
 * InputStream-based callers.
 * <p/>
 * This class is used to interface with callers using "classic" java.io.* APIs.
 * For code that manages the ByteBufferBackedInputStream, there are accessor
 * methods for the underlying buffer should it need to expand and contract on
 * reuse.
 * 
 */

@NotThreadsafe
public class ByteBufferBackedInputStream extends InputStream {

    private final ByteBufferContainer bufferContainer;

    public ByteBufferBackedInputStream(ByteBuffer buffer) {
        this.bufferContainer = new ByteBufferContainer(buffer);
    }

    public ByteBufferBackedInputStream(ByteBufferContainer bufferContainer) {
        this.bufferContainer = bufferContainer;
    }

    public ByteBuffer getBuffer() {
        return bufferContainer.getBuffer();
    }

    public void growBuffer() {
        bufferContainer.growBuffer();
    }

    @Override
    public int read() throws IOException {
        ByteBuffer buffer = bufferContainer.getBuffer();
        if(!buffer.hasRemaining())
            return -1;

        return buffer.get() & 0xff;
    }

    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {

        ByteBuffer buffer = bufferContainer.getBuffer();
        if(!buffer.hasRemaining())
            return -1;

        len = Math.min(len, buffer.remaining());
        buffer.get(bytes, off, len);
        return len;
    }

    @Override
    public void close() throws IOException {
        bufferContainer.close();
    }

    public void clear() {
        bufferContainer.reset();
    }
}
