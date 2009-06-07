package voldemort.server.niosocket;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

class ByteBufferBackedInputStream extends InputStream {

    private ByteBuffer buffer;

    public ByteBufferBackedInputStream(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public int read() throws IOException {
        if(!buffer.hasRemaining())
            return -1;

        return buffer.get();
    }

    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
        // Read only what's left
        len = Math.min(len, buffer.remaining());
        buffer.get(bytes, off, len);
        return len;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public void setBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

}
