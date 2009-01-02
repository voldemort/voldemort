package voldemort;

import java.io.*;
import java.util.concurrent.*;

public class BlockingInputStream extends InputStream {
    
    private BlockingQueue<Byte> queue;
    
    public BlockingInputStream() {
        this(new ArrayBlockingQueue<Byte>(1000));
    }
    
    public BlockingInputStream(BlockingQueue<Byte> queue) {
        this.queue = queue;
    }

    @Override
    public int read() throws IOException {
        try {
            return this.queue.take();
        } catch(InterruptedException e) {
            throw new IOException(e.getMessage());
        }
    }
    
}
