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
public class MappedFileReader extends BaseMappedFile implements Closeable {

    private static final Logger log = Logger.getLogger(MappedFileReader.class);

    protected FileInputStream in;

    protected MappedByteBuffer mappedByteBuffer = null;

    public MappedFileReader(String path) throws IOException {
        this(new File(path));
    }

    public MappedFileReader(File file) throws IOException {

        init(file);

    }

    private void init(File file) throws IOException {

        this.file = file;

        this.in = new FileInputStream(file);
        this.channel = in.getChannel();
        this.fd = Native.getFd(in.getFD());

        this.length = file.length();

    }

    /**
     * Read from this mapped file.
     */
    public MappedByteBuffer map(boolean setAutoLock) throws IOException {

        try {

            if(mappedByteBuffer == null) {

                if(setAutoLock) {
                    closer.add(new MemLock(file, in.getFD(), offset, length));
                }

                mappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, offset, length);

                closer.add(new MappedByteBufferCloser(mappedByteBuffer));

            }

            return mappedByteBuffer;

        } catch(IOException e) {

            log.error(String.format("Failed to map %s of length %,d at %,d",
                                    file.getPath(),
                                    length,
                                    offset), e);

            throw new IOException(String.format("Failed to map %s of length %,d at %,d",
                                                file.getPath(),
                                                length,
                                                offset), e);

        }

    }

    @Override
    public void close() throws IOException {

        if(closer.isClosed())
            return;

        closer.add(channel);
        closer.add(in);

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
