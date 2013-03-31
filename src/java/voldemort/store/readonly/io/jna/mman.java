package voldemort.store.readonly.io.jna;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;

public class mman {

    private static final Logger logger = Logger.getLogger(mman.class);
    public static final int PROT_READ = 0x1; /* Page can be read. */
    public static final int PROT_WRITE = 0x2; /* Page can be written. */
    public static final int PROT_EXEC = 0x4; /* Page can be executed. */
    public static final int PROT_NONE = 0x0; /* Page can not be accessed. */

    public static final int MAP_SHARED = 0x01; /* Share changes. */
    public static final int MAP_PRIVATE = 0x02; /* Changes are private. */

    public static final int MAP_ALIGN = 0x200; /* addr specifies alignment */

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

        Pointer result = Delegate.mmap(addr,
                                       new NativeLong(len),
                                       prot,
                                       flags,
                                       fildes,
                                       new NativeLong(off));

        if(Pointer.nativeValue(result) == -1) {
            if(logger.isDebugEnabled())
                logger.debug(errno.strerror());
        }

        return result;

    }

    public static int munmap(Pointer addr, long len) throws IOException {

        int result = Delegate.munmap(addr, new NativeLong(len));

        if(result != 0) {
            if(logger.isDebugEnabled())
                logger.debug(errno.strerror());
        }

        return result;

    }

    public static void mlock(Pointer addr, long len) throws IOException {

        int res = Delegate.mlock(addr, new NativeLong(len));
        if(res != 0) {
            if(logger.isDebugEnabled()) {
                logger.debug("Mlock failed probably because of insufficient privileges, errno:"
                             + errno.strerror() + ", return value:" + res);
            }
        } else {
            if(logger.isDebugEnabled())
                logger.debug("Mlock successfull");

        }

    }

    /**
     * Unlock the given region, throw an IOException if we fail.
     */
    public static void munlock(Pointer addr, long len) throws IOException {

        if(Delegate.munlock(addr, new NativeLong(len)) != 0) {
            if(logger.isDebugEnabled())
                logger.debug("munlocking failed with errno:" + errno.strerror());
        } else {
            if(logger.isDebugEnabled())
                logger.debug("munlocking region");
        }
    }

    static class Delegate {

        public static native Pointer mmap(Pointer addr,
                                          NativeLong len,
                                          int prot,
                                          int flags,
                                          int fildes,
                                          NativeLong off);

        public static native int munmap(Pointer addr, NativeLong len);

        public static native int mlock(Pointer addr, NativeLong len);

        public static native int munlock(Pointer addr, NativeLong len);

        static {
            Native.register("c");
        }

    }

    public static void main(String[] args) throws Exception {

        String path = args[0];

        File file = new File(path);
        FileInputStream in = new FileInputStream(file);
        int fd = voldemort.store.readonly.io.Native.getFd(in.getFD());
        if(logger.isDebugEnabled())
            logger.debug("File descriptor is: " + fd);

        // mmap a large file...
        Pointer addr = mmap(file.length(), PROT_READ, mman.MAP_SHARED | mman.MAP_ALIGN, fd, 0L);
        if(logger.isDebugEnabled())
            logger.debug("mmap address is: " + Pointer.nativeValue(addr));

        // try to mlock it directly
        mlock(addr, file.length());
        munlock(addr, file.length());

        munmap(addr, file.length());

    }
}
