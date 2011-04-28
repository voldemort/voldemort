package voldemort.store.readonly.mr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.EOFException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.Test;

import voldemort.TestUtils;
import voldemort.utils.ByteUtils;

public class HdfsDataFileChunkTest {

    @Test
    public void testDataFileChunk() throws IOException {
        Logger.getRootLogger().removeAllAppenders();

        Path path = new Path(TestUtils.createTempDir().getAbsolutePath(), "tempFile");
        FileSystem fs = path.getFileSystem(new Configuration());
        fs.create(path);
        HdfsDataFileChunk chunk = new HdfsDataFileChunk(fs, fs.getFileStatus(path));

        int jumps = 10;
        ByteBuffer buffer = ByteBuffer.allocate(jumps);
        try {
            chunk.read(buffer, 0);
            fail("Should have thrown EOFException");
        } catch(EOFException e) {}

        for(int numBytes = 100; numBytes <= 1000; numBytes += 100) {
            // Clear up file
            fs.delete(path, true);
            fs.create(path);

            // Write random bytes to it
            byte[] randomBytes = TestUtils.randomBytes(numBytes);

            FileOutputStream stream = new FileOutputStream(path.toString());
            stream.write(randomBytes);
            stream.close();

            // Create chunk set
            chunk = new HdfsDataFileChunk(fs, fs.getFileStatus(path));

            // Read the chunks
            for(int offset = 0; offset < numBytes; offset += jumps) {
                buffer.clear();
                assertEquals(chunk.read(buffer, offset), jumps);
                assertEquals(0, ByteUtils.compare(buffer.array(), ByteUtils.copy(randomBytes,
                                                                                 offset,
                                                                                 offset + jumps)));
            }

        }

    }
}
