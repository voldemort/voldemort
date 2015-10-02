package voldemort.utils;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Static utility functions to deal with exceptions.
 */
public class ExceptionUtils {
    /**
     * Inspects a given {@link Throwable} as well as its nested causes, in order to look
     * for a specific exception class.
     *
     * @return true if a the throwableToInspect corresponds to or is caused by the throwableClassToLookFor
     */
    public static boolean recursiveClassEquals(Throwable throwableToInspect, Class throwableClassToLookFor) {
        if (throwableToInspect.getClass().equals(throwableClassToLookFor)) {
            return true;
        } else {
            Throwable cause = throwableToInspect.getCause();
            return cause != null && recursiveClassEquals(cause, throwableClassToLookFor);
        }
    }

    /**
     * @return a String representation of the provided throwable's stacktrace.
     */
    public static String stackTraceToString(Throwable throwable) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        throwable.printStackTrace(pw);
        return sw.toString(); // stack trace as a string
    }
}
