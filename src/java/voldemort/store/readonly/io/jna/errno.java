package voldemort.store.readonly.io.jna;

import com.sun.jna.Library;
import com.sun.jna.Native;

public class errno {

    private static InterfaceDelegate delegate = (InterfaceDelegate) Native.loadLibrary("c",
                                                                                       InterfaceDelegate.class);

    /**
     * The routine perror() produces a message on the standard error output,
     * describing the last error encountered during a call to a system or
     * library function. First (if s is not NULL and *s is not a null byte
     * ('\0')) the argument string s is printed, followed by a colon and a
     * blank. Then the message and a new-line.
     * 
     * To be of most use, the argument string should include the name of the
     * function that incurred the error. The error number is taken from the
     * external variable errno, which is set when errors occur but not cleared
     * when non-erroneous calls are made.
     * 
     * The global error list sys_errlist[] indexed by errno can be used to
     * obtain the error message without the newline. The largest message number
     * provided in the table is sys_nerr -1. Be careful when directly accessing
     * this list because new error values may not have been added to
     * sys_errlist[].
     * 
     * When a system call fails, it usually returns -1 and sets the variable
     * errno to a value describing what went wrong. (These values can be found
     * in <errno.h>.) Many library functions do likewise. The function perror()
     * serves to translate this error code into human-readable form. Note that
     * errno is undefined after a successful library call: this call may well
     * change this variable, even though it succeeds, for example because it
     * internally used some other library function that failed. Thus, if a
     * failing call is not immediately followed by a call to perror(), the value
     * of errno should be saved.
     */
    public static int perror(String s) {
        return delegate.perror(s);
    }

    /**
     * The strerror() function returns a string describing the error code passed
     * in the argument errnum, possibly using the LC_MESSAGES part of the
     * current locale to select the appropriate language. This string must not
     * be modified by the application, but may be modified by a subsequent call
     * to perror() or strerror(). No library function will modify this string.
     * 
     * The strerror_r() function is similar to strerror(), but is thread safe.
     * This function is available in two versions: an XSI-compliant version
     * specified in POSIX.1-2001, and a GNU-specific version (available since
     * glibc 2.0). If _XOPEN_SOURCE is defined with the value 600, then the
     * XSI-compliant version is provided, otherwise the GNU-specific version is
     * provided.
     * 
     * The XSI-compliant strerror_r() is preferred for portable applications. It
     * returns the error string in the user-supplied buffer buf of length
     * buflen.
     * 
     * The GNU-specific strerror_r() returns a pointer to a string containing
     * the error message. This may be either a pointer to a string that the
     * function stores in buf, or a pointer to some (immutable) static string
     * (in which case buf is unused). If the function stores a string in buf,
     * then at most buflen bytes are stored (the string may be truncated if
     * buflen is too small) and the string always includes a terminating null
     * byte.
     * 
     */
    public static String strerror(int errnum) {
        return delegate.strerror(errnum);
    }

    public static String strerror() {
        return strerror(errno());
    }

    /**
     * The <errno.h> header file defines the integer variable errno, which is
     * set by system calls and some library functions in the event of an error
     * to indicate what went wrong. Its value is significant only when the call
     * returned an error (usually -1), and a function that does succeed is
     * allowed to change errno.
     * 
     * Sometimes, when -1 is also a valid successful return value one has to
     * zero errno before the call in order to detect possible errors.
     * 
     * errno is defined by the ISO C standard to be a modifiable lvalue of type
     * int, and must not be explicitly declared; errno may be a macro. errno is
     * thread-local; setting it in one thread does not affect its value in any
     * other thread.
     * 
     * Valid error numbers are all non-zero; errno is never set to zero by any
     * library function. All the error names specified by POSIX.1 must have
     * distinct values, with the exception of EAGAIN and EWOULDBLOCK, which may
     * be the same.
     * 
     * Below is a list of the symbolic error names that are defined on Linux.
     * Some of these are marked POSIX.1, indicating that the name is defined by
     * POSIX.1-2001, or C99, indicating that the name is defined by C99.
     * 
     */
    public static int errno() {
        return Native.getLastError();
    }

    interface InterfaceDelegate extends Library {

        int perror(String s);

        String strerror(int errnum);

    }

}
