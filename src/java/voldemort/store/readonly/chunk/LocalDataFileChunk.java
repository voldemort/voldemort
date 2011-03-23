package voldemort.store.readonly.chunk;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class LocalDataFileChunk implements DataFileChunk {

    public FileChannel dataFile;

    public LocalDataFileChunk(FileChannel dataFile) {
        this.dataFile = dataFile;
    }

    public int read(ByteBuffer buffer, long currentOffset) throws IOException {
        return dataFile.read(buffer, currentOffset);
    }

}
