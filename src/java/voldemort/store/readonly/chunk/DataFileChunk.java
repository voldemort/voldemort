package voldemort.store.readonly.chunk;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Common interface to abstract out reading from Hdfs and Local files
 * 
 */
public interface DataFileChunk {

    public int read(ByteBuffer buffer, long currentOffset) throws IOException;
}
