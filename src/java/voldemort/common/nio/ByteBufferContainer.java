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

    private void assignBuffer(ByteBuffer newBuffer) {
        if(sizeTracker != null) {
            int oldSize = 0;
            if(this.buffer != null ){
                oldSize = this.buffer.capacity();
            }
            updateSizeStats(oldSize, newBuffer.capacity());
        }
        this.buffer = newBuffer;
    }

    public ByteBufferContainer(int sizeLowerBound, int sizeUpperBound, MutableLong sizeTracker) {
        this.sizeTracker = sizeTracker;

        this.sizeLowerBound = sizeLowerBound;
        this.sizeUpperBound = sizeUpperBound;

        isClosed = new AtomicBoolean(false);

        assignBuffer(ByteBuffer.allocate(sizeLowerBound));
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
        if(newSize > oldSize) {
            assignBuffer(ByteUtils.expand(this.buffer, newSize));
        }
    }

    public void ensureSpace(int writeLen) {
        int need = (writeLen - this.buffer.remaining());

        if(need <= 0) {
            return;
        }

        growBuffer(buffer.capacity() + need);
    }

    public ByteBuffer getBuffer() {
        return this.buffer;
    }

    public void reset() {
        if(this.buffer.capacity() > sizeUpperBound) {
            assignBuffer(ByteBuffer.allocate(sizeLowerBound));
        }
        this.buffer.clear();
    }

    public void close() {
        if(isClosed.compareAndSet(false, true)) {
            updateSizeStats(buffer.capacity(), 0);
        }
    }

    private void updateSizeStats(int oldSize, int newSize) {
        if(sizeTracker != null) {
            sizeTracker.subtract(oldSize);
            sizeTracker.add(newSize);
        }
    }
}
