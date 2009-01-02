package voldemort;

import java.io.*;
import java.util.concurrent.*;

public class BlockingOutputStream extends OutputStream {
    
    private BlockingQueue<Byte> queue;
    
    public BlockingOutputStream() {
        this(new ArrayBlockingQueue<Byte>(1000));
    }

    public BlockingOutputStream(BlockingQueue<Byte> queue) {
        super();
        this.queue = queue;
    }

    @Override
    public void write(int b) throws IOException {
        try {
            queue.put((byte) b);
        } catch(InterruptedException e) {
            throw new IOException(e.getMessage());
        }
    }
    
}
