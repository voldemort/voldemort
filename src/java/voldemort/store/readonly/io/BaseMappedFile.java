package voldemort.store.readonly.io;

import java.io.File;
import java.nio.channels.FileChannel;

/**
 *
 */
public class BaseMappedFile {

    protected FileChannel channel;

    protected long offset = 0;

    protected long length = 0;

    protected Closer closer = new Closer();

    protected File file;

    protected int fd;

    protected boolean fadvise = true;

    public File getFile() {
        return file;
    }

    public int getFd() {
        return fd;
    }

    public boolean isClosed() {
        return closer.isClosed();
    }

    @Override
    public String toString() {
        return file.toString();
    }

}
