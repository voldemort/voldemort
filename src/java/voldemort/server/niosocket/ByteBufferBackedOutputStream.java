package voldemort.server.niosocket;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

class ByteBufferBackedOutputStream extends OutputStream {

    private ByteBuffer buffer;

    public ByteBufferBackedOutputStream(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public void write(int b) throws IOException {
        buffer.put((byte) b);
    }

    @Override
    public void write(byte[] bytes, int off, int len) throws IOException {
        buffer.put(bytes, off, len);
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public void setBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

}
