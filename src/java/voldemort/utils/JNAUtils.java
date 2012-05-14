package voldemort.utils;

import org.apache.log4j.Logger;

import com.sun.jna.LastErrorException;
import com.sun.jna.Native;

/**
 * Native functions used through JNA
 * 
 */
public class JNAUtils {

    private static final Logger logger = Logger.getLogger(JNAUtils.class);

    /* Flags for mlock_all */
    private static final int MCL_CURRENT = 1;
    private static final int MCL_FUTURE = 2;

    private static final int ENOMEM = 12;

    static {
        try {
            Native.register("c");
        } catch(NoClassDefFoundError e) {
            logger.info("Could not locate JNA classes");
        } catch(UnsatisfiedLinkError e) {
            logger.info("Failed to link to native library");
        } catch(NoSuchMethodError e) {
            logger.warn("Older version of JNA. Please upgrade to 3.2.7+");
        }
    }

    private static native int mlockall(int flags) throws LastErrorException;

    private static native int munlockall() throws LastErrorException;

    private static boolean isOperatingSystem(String os) {
        if(System.getProperty("os.name").toLowerCase().contains(os))
            return true;
        else
            return false;
    }

    public static void tryMlockall() {
        try {
            if(isOperatingSystem("windows"))
                return;
            // Since we demand-zero every page of the heap while bringing up the
            // jvm, MCL_FUTURE is not needed
            mlockall(MCL_CURRENT);
            logger.info("mlockall() on JVM Heap successful");
        } catch(Exception e) {
            if(!(e instanceof LastErrorException))
                logger.error("Unexpected error during mlock of server heap", e);

            LastErrorException le = (LastErrorException) e;
            if(le.getErrorCode() == ENOMEM && isOperatingSystem("linux")) {
                logger.warn("Unable to lock JVM memory (ENOMEM)."
                            + " This can result in part of the JVM being swapped out with higher Young gen stalls"
                            + " Increase RLIMIT_MEMLOCK or run Voldemort as root.");
            } else if(!isOperatingSystem("mac")) {
                // fixes a OS X oddity, where it still throws an error, even
                // though mlockall succeeds
                logger.warn("Unknown mlockall error " + le.getErrorCode());
            }
        }
    }

    public static void tryMunlockall() {
        try {
            if(isOperatingSystem("windows"))
                return;
            munlockall();
            logger.info("munlockall() on JVM Heap successful");
        } catch(Exception e) {
            if(!(e instanceof LastErrorException))
                logger.error("Unexpected error during mlock of server heap", e);
            LastErrorException le = (LastErrorException) e;
            logger.warn("Error unlocking JVM heap  " + le.getErrorCode());
        }
    }

}
