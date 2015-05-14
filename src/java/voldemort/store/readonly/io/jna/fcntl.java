package voldemort.store.readonly.io.jna;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.sun.jna.Native;

public class fcntl {

    public static final int POSIX_FADV_NORMAL = 0; /* fadvise.h */
    public static final int POSIX_FADV_RANDOM = 1; /* fadvise.h */
    public static final int POSIX_FADV_SEQUENTIAL = 2; /* fadvise.h */
    public static final int POSIX_FADV_WILLNEED = 3; /* fadvise.h */
    public static final int POSIX_FADV_DONTNEED = 4; /* fadvise.h */
    public static final int POSIX_FADV_NOREUSE = 5; /* fadvise.h */

    private static final Logger logger = Logger.getLogger(fcntl.class);

    /**
     * <b>posix documentation</b>
     * 
     * Actual Linux implementation resides here:
     * 
     * http://lxr.linux.no/linux+v3.0.3/mm/fadvise.c#L77
     * 
     * <p>
     * posix_fadvise - predeclare an access pattern for file data
     * 
     * <p>
     * Synopsis
     * 
     * <p>
     * #include <fcntl.h>
     * 
     * <p>
     * int posix_fadvise(int fd, off_t offset, off_t len, int advice);
     * 
     * <p>
     * Feature Test Macro Requirements for glibc (see feature_test_macros(7)):
     * posix_fadvise(): _XOPEN_SOURCE >= 600 || _POSIX_C_SOURCE >= 200112L
     * 
     * <p>
     * Description
     * 
     * <p>
     * Programs can use posix_fadvise() to announce an intention to access file
     * data in a specific pattern in the future, thus allowing the kernel to
     * perform appropriate optimizations. The advice applies to a (not
     * necessarily existent) region starting at offset and extending for len
     * bytes (or until the end of the file if len is 0) within the file referred
     * to by fd. The advice is not binding; it merely constitutes an expectation
     * on behalf of the application.
     * 
     * <p>
     * Permissible values for advice include:
     * 
     * <p>
     * POSIX_FADV_NORMAL
     * 
     * <p>
     * Indicates that the application has no advice to give about its access
     * pattern for the specified data. If no advice is given for an open file,
     * this is the default assumption.
     * 
     * <p>
     * POSIX_FADV_SEQUENTIAL
     * 
     * <p>
     * The application expects to access the specified data sequentially (with
     * lower offsets read before higher ones).
     * 
     * <p>
     * POSIX_FADV_RANDOM
     * 
     * <p>
     * The specified data will be accessed in random order.
     * 
     * <p>
     * POSIX_FADV_NOREUSE
     * 
     * <p>
     * The specified data will be accessed only once.
     * 
     * <p>
     * POSIX_FADV_WILLNEED
     * 
     * <p>
     * The specified data will be accessed in the near future.
     * 
     * <p>
     * POSIX_FADV_DONTNEED
     * 
     * <p>
     * The specified data will not be accessed in the near future.
     * 
     * <p>
     * Return Value
     * 
     * On success, zero is returned. On error, an error number is returned.
     * 
     * <b>java documentation</b>
     * 
     * We do not return -1 if we fail but instead throw an IOException
     * 
     * @throws IOException if this call fails.
     */
    public static int posix_fadvise(int fd, long offset, long len, int advice) throws IOException {

        int result = Delegate.posix_fadvise(fd, offset, len, advice);

        if(result != 0)
            throw new IOException(errno.strerror(result));

        return result;

    }

    /**
     * <b>posix documentation</b>
     * 
     * <p>
     * The function posix_fallocate() ensures that disk space is allocated for
     * the file referred to by the descriptor fd for the bytes in the range
     * starting at offset and continuing for len bytes. After a successful call
     * to posix_fallocate(), subsequent writes to bytes in the specified range
     * are guaranteed not to fail because of lack of disk space.
     * 
     * <p>
     * If the size of the file is less than offset+len, then the file is
     * increased to this size; otherwise the file size is left unchanged.
     * 
     * <p>
     * Return Value
     * 
     * <p>
     * posix_fallocate() returns zero on success, or an error number on failure.
     * Note that errno is not set.
     * 
     * <b>java documentation</b>
     * 
     * We do not return -1 if we fail but instead throw an IOException
     * 
     * @throws IOException if this call fails.
     * 
     */
    public static int posix_fallocate(int fd, long offset, long len) throws IOException {

        int result = Delegate.posix_fallocate(fd, offset, len);

        if(result != 0) {
            logger.warn(errno.strerror(result));
        }

        return result;

    }

    static class Delegate {

        public static native int posix_fadvise(int fd, long offset, long len, int advice);

        public static native int posix_fallocate(int fd, long offset, long len);

        static {
            Native.register("c");
        }

    }

}
