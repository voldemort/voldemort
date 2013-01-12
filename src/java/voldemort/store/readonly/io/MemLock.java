package voldemort.store.readonly.io;

import java.io.Closeable;
import java.io.File;
import java.io.FileDescriptor;
import java.io.IOException;

import org.apache.log4j.Logger;

import voldemort.store.readonly.io.jna.mman;

import com.sun.jna.Pointer;

public class MemLock implements Closeable {

    private static final Logger logger = Logger.getLogger(MemLock.class);

    private Pointer pa;
    private long length;
    private File file;
    private FileDescriptor descriptor;

    /**
     * Call mmap a file descriptor, then lock the pages with MAP_LOCKED. This
     * will then prevent the pages from being swapped out due to VFS cache
     * pressure.
     * 
     * @param descriptor The file we should mmap and MAP_LOCKED
     * @param offset
     * @param length
     * @see #close()
     * @throws IOException
     */
    public MemLock(File file, FileDescriptor descriptor, long offset, long length)
                                                                                  throws IOException {
        if(logger.isDebugEnabled())
            logger.debug("mlocking " + file + " with length " + length);

        this.setFile(file);
        this.setDescriptor(descriptor);
        this.length = length;

        int fd = voldemort.store.readonly.io.Native.getFd(descriptor);

        pa = mman.mmap(length, mman.PROT_READ, mman.MAP_SHARED | mman.MAP_ALIGN, fd, offset);

        // even though technically we have specified MAP_LOCKED this isn't
        // supported on OpenSolaris or older Linux kernels (or OS X).

        mman.mlock(pa, length);

    }

    /**
     * Release this lock so that the memory can be returned to the OS if it
     * wants to us it.
     */
    @Override
    public void close() throws IOException {

        mman.munlock(pa, length);
        mman.munmap(pa, length);

        if(logger.isDebugEnabled())
            logger.debug("munlocking " + file + " with length " + length);

    }

    public File getFile() {
        return file;
    }

    public void setFile(File file) {
        this.file = file;
    }

    public FileDescriptor getDescriptor() {
        return descriptor;
    }

    public void setDescriptor(FileDescriptor descriptor) {
        this.descriptor = descriptor;
    }

}
