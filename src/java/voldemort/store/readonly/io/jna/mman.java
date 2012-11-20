package voldemort.store.readonly.io.jna;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.sun.jna.Native;
import com.sun.jna.Pointer;

public class mman {

    private static final Logger logger = Logger.getLogger(mman.class);
    public static final int PROT_READ = 0x1; /* Page can be read. */
    public static final int PROT_WRITE = 0x2; /* Page can be written. */
    public static final int PROT_EXEC = 0x4; /* Page can be executed. */
    public static final int PROT_NONE = 0x0; /* Page can not be accessed. */

    public static final int MAP_SHARED = 0x01; /* Share changes. */
    public static final int MAP_PRIVATE = 0x02; /* Changes are private. */

    public static final int MAP_LOCKED = 0x02000; /* Lock the mapping. */

    // http://linux.die.net/man/2/mmap
    // http://www.opengroup.org/sud/sud1/xsh/mmap.htm
    // http://linux.die.net/include/sys/mman.h
    // http://linux.die.net/include/bits/mman.h

    // off_t = 8
    // size_t = 8
    public static Pointer mmap(long len, int prot, int flags, int fildes, long off)
            throws IOException {

        // we don't really have a need to change the recommended pointer.
        Pointer addr = new Pointer(0);

        Pointer result = Delegate.mmap(addr, len, prot, flags, fildes, off);

        if(Pointer.nativeValue(result) == -1) {

            logger.warn(errno.strerror());
        }

        return result;

    }

    public static int munmap(Pointer addr, long len) throws IOException {

        int result = Delegate.munmap(addr, len);

        if(result != 0) {
            logger.warn(errno.strerror());
        }

        return result;

    }

    public static void mlock(Pointer addr, long len) throws IOException {

        if(Delegate.mlock(addr, len) != 0) {
            logger.warn("Mlock failed probably because of insufficient privileges");
            logger.warn(errno.strerror());
        } else {
            logger.info("Mlock successfull");

        }

    }

    /**
     * Unlock the given region, throw an IOException if we fail.
     */
    public static void munlock(Pointer addr, long len) throws IOException {

        if(Delegate.munlock(addr, len) != 0) {
            logger.warn(errno.strerror());
        } else {
            logger.info("munlocking region");
        }

    }

    static class Delegate {

        public static native Pointer mmap(Pointer addr,
                                          long len,
                                          int prot,
                                          int flags,
                                          int fildes,
                                          long off);

        public static native int munmap(Pointer addr, long len);

        public static native int mlock(Pointer addr, long len);

        public static native int munlock(Pointer addr, long len);

        static {
            Native.register("c");
        }

    }

    public static void main(String[] args) throws Exception {

        String path = args[0];

        File file = new File(path);
        FileInputStream in = new FileInputStream(file);
        int fd = voldemort.store.readonly.io.Native.getFd(in.getFD());

        // mmap a large file...
        Pointer addr = mmap(file.length(), PROT_READ, mman.MAP_SHARED | mman.MAP_LOCKED, fd, 0);

        // try to mlock it directly
        mlock(addr, file.length());
        munlock(addr, file.length());

        munmap(addr, file.length());

    }

}
