package voldemort.store.readonly.io;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.log4j.Logger;

import voldemort.store.readonly.io.jna.fcntl;

/**
 * Facade around a MappedByteBuffer but we also support mlock on the mapped
 * pages, and closing all dependent resources.
 * 
 */
public class MappedFileReader extends BaseMappedFile implements Closeable {

    private static final Logger log = Logger.getLogger(MappedFileReader.class);

    public static boolean DEFAULT_AUTO_LOCK = true;

    protected FileInputStream in;

    // TODO expose this as a server side config
    protected boolean autoLock = DEFAULT_AUTO_LOCK;

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
    public MappedByteBuffer map() throws IOException {

        try {

            // in JDK 1.6 and earlier the max mmap we could have was 2GB so to
            // route around this problem we create a number of smaller mmap
            // files , one per 2GB region and then use a composite channel
            // buffer

            if(mappedByteBuffer == null) {

                if(autoLock) {
                    closer.add(new MemLock(file, in.getFD(), offset, length));
                }

                mappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, offset, length);

                closer.add(new MappedByteBufferCloser(mappedByteBuffer));

            }

            return mappedByteBuffer;

        } catch(IOException e) {

            throw new IOException(String.format("Failed to map %s of length %,d at %,d",
                                                file.getPath(),
                                                length,
                                                offset), e);

        }

    }

    public boolean getAutoLock() {
        return this.autoLock;
    }

    public void setAutoLock(boolean autoLock) {
        this.autoLock = autoLock;
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

            if(fadvise) {
                fcntl.posix_fadvise(fd, offset, length, fcntl.POSIX_FADV_RANDOM);
            }

        }

    }

}
