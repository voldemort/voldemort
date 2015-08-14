package voldemort.store.readonly.io;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.log4j.Logger;

/**
 * Facade around a MappedByteBuffer but we also support mlock on the mapped
 * pages, and closing all dependent resources.
 * 
 */
public class MappedFileReader implements Closeable {

    private Closer closer = new Closer();

    private File file;

    public File getFile() {
        return file;
    }

    public boolean isClosed() {
        return closer.isClosed();
    }

    @Override
    public String toString() {
        return file.toString();
    }

    private static final Logger log = Logger.getLogger(MappedFileReader.class);

    private MappedByteBuffer mappedByteBuffer = null;

    public MappedFileReader(File file) {
        this.file = file;
    }

    /**
     * Return a new MappedByteBuffer which can be used to read
     * data from the file.  If setAutoLock is true, attempt to
     * lock the file's data in memory.  Calling map() multiple
     * times returns the same MappedByteBuffer.
     */
    public MappedByteBuffer map() throws IOException {

        long length = -1;
        try {

            if(mappedByteBuffer == null) {
                /* Sample the file length just before we mmap() the
                 * file, as it may have changed size since the ctor. We
                 * do need to remember the exact length we mmap()ed for
                 * later when we unmap() to avoid leaking mappings, but
                 * MappedByteBuffer does that for us.
                 */
                length = file.length();
                FileInputStream in = new FileInputStream(file);
                FileChannel channel = in.getChannel();
                mappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, length);

                closer.add(new MappedByteBufferCloser(mappedByteBuffer));

                /* On Unix-like systems we don't need to keep the file
                 * descriptor around after we mmap()ed the file.  So
                 * close it now. */
                channel.close();
                in.close();
            }

            return mappedByteBuffer;

        } catch(IOException e) {
            log.error(String.format("Failed to map %s of length %,d",
                                    file.getPath(), length), e);

            throw new IOException(String.format("Failed to map %s of length %,d",
                                                file.getPath(),
                                                length), e);
        }
    }

    @Override
    public void close() throws IOException {
        if(closer.isClosed())
            return;
        closer.close();
    }

    /**
     * A closeable which is smart enough to work on mapped byte buffers.
     */
    class MappedByteBufferCloser extends ByteBufferCloser {

        public MappedByteBufferCloser(ByteBuffer buff) {
            super(buff);
        }

        @Override
        public void close() throws IOException {
            super.close();
        }
    }
}
