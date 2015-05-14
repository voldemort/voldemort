package voldemort.store.readonly.io;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A closeable which is smart enough to work on byte buffers.
 */
public class ByteBufferCloser implements Closeable {

    private ByteBuffer buff;

    public ByteBufferCloser(ByteBuffer buff) {
        this.buff = buff;
    }

    @Override
    public void close() throws IOException {

        sun.misc.Cleaner cl = ((sun.nio.ch.DirectBuffer) buff).cleaner();

        if(cl != null) {
            cl.clean();
        }

    }

}
