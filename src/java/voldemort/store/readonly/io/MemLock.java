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

    /**
     * Call mmap a file descriptor, then lock the pages using mlock(). This
     * will then prevent the pages from being swapped out due to VFS cache
     * pressure.
     * 
     * @param descriptor The file we should mmap and mlock
     * @param offset
     * @param length
     * @see #close()
     * @throws IOException
     */
    public MemLock(File file, FileDescriptor descriptor, long offset, long length)
                                                                                  throws IOException {
        if(logger.isDebugEnabled())
            logger.debug("mlocking " + file + " with length " + length);

        this.file = file;
        this.length = length;
	pa = null;

        int fd = voldemort.store.readonly.io.Native.getFd(descriptor);

        pa = mman.mmap(length, mman.PROT_READ, mman.MAP_SHARED, fd, offset);

	/* note, any failure in the mlock() system call is ignored,
	 * because it's normal behavior in an unprivileged process is to
	 * fail due system rlimits.
	 */
	mman.mlock(pa, length);

    }

    /**
     * Release this lock so that the memory can be returned to the OS if it
     * wants to us it.
     */
    @Override
    public void close() throws IOException {

        if(logger.isDebugEnabled())
            logger.debug("munlocking " + file + " with length " + length);

	if(pa != null) {
	    /* note: any failure in the munlock() system call is ignored too */
	    Pointer xpa = pa;
	    pa = null;
	    mman.munlock(xpa, length);
	    mman.munmap(xpa, length);
	}
    }

    public File getFile() {
        return file;
    }

}
