package voldemort.store.readonly.mr;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import voldemort.store.readonly.chunk.DataFileChunk;

public class HdfsDataFileChunk implements DataFileChunk {

    private FSDataInputStream fileStream;

    public HdfsDataFileChunk(FileSystem fs, FileStatus dataFile) throws IOException {
        this.fileStream = fs.open(dataFile.getPath());
    }

    public int read(ByteBuffer buffer, long currentOffset) throws IOException {
        byte[] bufferByte = new byte[buffer.capacity()];
        this.fileStream.readFully(currentOffset, bufferByte);
        buffer.put(bufferByte);
        return buffer.capacity();
    }
}
