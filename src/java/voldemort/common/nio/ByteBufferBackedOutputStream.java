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

import org.apache.commons.lang.mutable.MutableLong;

import voldemort.annotations.concurrency.NotThreadsafe;
import voldemort.utils.ByteUtils;

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

    private ByteBuffer buffer;

    private boolean wasExpanded;

    /**
     * Reference to a size tracking object, that tracks the size of the buffer
     * in bytes
     */
    private MutableLong sizeTracker;

    public ByteBufferBackedOutputStream(ByteBuffer buffer) {
        this.buffer = buffer;
        wasExpanded = false;
        this.sizeTracker = null;
    }

    public ByteBufferBackedOutputStream(ByteBuffer buffer, MutableLong sizeTracker) {
        this.buffer = buffer;
        wasExpanded = false;
        this.sizeTracker = sizeTracker;
        if(buffer != null)
            this.sizeTracker.add(buffer.capacity());
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public void setBuffer(ByteBuffer newBuffer) {
        // update the size tracker with the new buffer size
        if((sizeTracker != null && this.buffer != null && newBuffer != null)) {
            sizeTracker.add(newBuffer.capacity());
            sizeTracker.subtract(this.buffer.capacity());
        }
        this.buffer = newBuffer;
        wasExpanded = false;
    }

    @Override
    public void write(int b) throws IOException {
        expandIfNeeded(1);
        buffer.put((byte) b);
    }

    @Override
    public void write(byte[] bytes, int off, int len) throws IOException {
        expandIfNeeded(len);
        buffer.put(bytes, off, len);
    }

    private void expandIfNeeded(int len) {
        int need = len - buffer.remaining();

        if(need <= 0)
            return;

        int newCapacity = (buffer.capacity() + need) * 2;
        // update the size tracker with the new buffer size
        if(sizeTracker != null) {
            sizeTracker.add(newCapacity);
            sizeTracker.subtract(this.buffer.capacity());
        }
        buffer = ByteUtils.expand(buffer, newCapacity);
        wasExpanded = true;
    }

    public boolean wasExpanded() {
        return wasExpanded;
    }

    public void close() {
        if(sizeTracker != null && this.buffer != null) {
            sizeTracker.subtract(this.buffer.capacity());
        }
    }
}
