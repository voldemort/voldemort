package voldemort.utils;

/**
 * Static utility functions to deal with exceptions.
 */
public class ExceptionUtils {
    public static boolean recursiveClassEquals(Throwable throwableToInspect, Class throwableToLookFor) {
        if (throwableToInspect.getClass().equals(throwableToLookFor)) {
            return true;
        } else {
            Throwable cause = throwableToInspect.getCause();
            return cause != null && recursiveClassEquals(cause, throwableToLookFor);
        }
    }
}
