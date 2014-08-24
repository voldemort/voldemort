package voldemort.common.nio;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.mutable.MutableLong;

import voldemort.utils.ByteUtils;

/*
 * ByteBufferContainer holds a reference to the ByteBuffer. This lets your share
 * the byte buffer with the input stream and the output stream when only one of
 * them can be in progress. For Voldemort this is always true, but most of the
 * admin code, does not clear the buffer once the contents are read in stream
 * processing. Hence the client and server can use the same buffer for read and
 * write to begin with.
 */
public class ByteBufferContainer {

    private ByteBuffer buffer;
    private final int sizeLowerBound;
    private final int sizeUpperBound;

    /**
     * Reference to a size tracking object, that tracks the size of the buffer
     * in bytes
     */
    private final MutableLong sizeTracker;
    private final AtomicBoolean isClosed;

    public ByteBufferContainer(int sizeLowerBound, int sizeUpperBound, MutableLong sizeTracker) {
        this.buffer = ByteBuffer.allocate(sizeLowerBound);
        updateSizeStats(0, this.buffer.capacity());
        this.sizeLowerBound = sizeLowerBound;
        this.sizeUpperBound = sizeUpperBound;

        this.sizeTracker = sizeTracker;
        isClosed = new AtomicBoolean(false);
    }

    public ByteBufferContainer(ByteBuffer byteBuffer) {
        if(byteBuffer == null) {
            throw new IllegalArgumentException("byteBuffer is null");
        }
        this.buffer = byteBuffer;
        this.sizeLowerBound = byteBuffer.capacity();
        this.sizeUpperBound = byteBuffer.capacity() * 2;

        this.sizeTracker = null;

        isClosed = new AtomicBoolean(false);
    }

    public void growBuffer() {
        growBuffer(buffer.capacity() * 2);
    }

    public void growBuffer(int newSize) {
        int oldSize = buffer.capacity();
        this.buffer = ByteUtils.expand(this.buffer, newSize);
        updateSizeStats(oldSize, this.buffer.capacity());
    }

    public ByteBuffer getBuffer() {
        return this.buffer;
    }

    public void reset() {
        int currentSize = this.buffer.capacity();
        if(this.buffer.capacity() > sizeUpperBound) {
            this.buffer = ByteBuffer.allocate(sizeLowerBound);
            updateSizeStats(currentSize, buffer.capacity());
        }
        this.buffer.clear();
    }

    public void close() {
        if(isClosed.compareAndSet(false, true)) {
            if(sizeTracker != null) {
                updateSizeStats(buffer.capacity(), 0);
            }
        }
    }

    private void updateSizeStats(int oldSize, int newSize) {
        if(sizeTracker != null) {
            sizeTracker.subtract(oldSize);
            sizeTracker.add(newSize);
        }
    }

}
