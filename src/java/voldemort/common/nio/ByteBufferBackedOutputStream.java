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
import java.io.OutputStream;
import java.nio.ByteBuffer;

import voldemort.annotations.concurrency.NotThreadsafe;

/**
 * ByteBufferBackedOutputStream serves two purposes:
 * <ol>
 * <li>It allows a ByteBuffer to be the source of data for OutputStream-based
 * callers
 * <li>It expands the underlying buffer to accommodate incoming data
 * </ol>
 * This class is used to interface with callers using "classic" java.io.* APIs.
 * This OutputStream provides the means to grow the buffer on demand as more
 * data is written by those callers. For code that manages the
 * ByteBufferBackedOutputStream, there are accessor methods for the underlying
 * buffer as the object reference passed into the constructor is changed upon
 * expansion. Additionally, some callers might wish to "un-expand" the buffer
 * back to a more reasonable size after use.
 * 
 */

@NotThreadsafe
public class ByteBufferBackedOutputStream extends OutputStream {

    private final ByteBufferContainer bufferContainer;
    private boolean wasExpanded = false;

    public ByteBufferBackedOutputStream(ByteBuffer buffer) {
        this.bufferContainer = new ByteBufferContainer(buffer);
    }

    public ByteBufferBackedOutputStream(ByteBufferContainer bufferContainer) {
        this.bufferContainer = bufferContainer;
    }

    public ByteBuffer getBuffer() {
        return bufferContainer.getBuffer();
    }

    public void growBuffer() {
        bufferContainer.growBuffer();
    }

    @Override
    public void write(int b) throws IOException {
        ByteBuffer buffer = expandIfNeeded(1);
        buffer.put((byte) b);
    }

    @Override
    public void write(byte[] bytes, int off, int len) throws IOException {
        ByteBuffer buffer = expandIfNeeded(len);
        buffer.put(bytes, off, len);
    }

    private ByteBuffer expandIfNeeded(int len) {
        ByteBuffer buffer = bufferContainer.getBuffer();
        int need = len - buffer.remaining();

        if(need <= 0)
            return buffer;

        wasExpanded = true;
        int newCapacity = (buffer.capacity() + need) * 2;
        bufferContainer.growBuffer(newCapacity);

        return bufferContainer.getBuffer();
    }

    public boolean wasExpanded() {
        return wasExpanded;
    }

    @Override
    public void close() throws IOException {
        bufferContainer.close();
        super.close();
    }

    public void clear() {
        wasExpanded = false;
        bufferContainer.reset();
    }
}
