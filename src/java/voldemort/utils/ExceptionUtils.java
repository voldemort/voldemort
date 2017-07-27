package voldemort.utils;

import voldemort.VoldemortException;
import voldemort.server.protocol.admin.AsyncOperationNotFoundException;
import voldemort.server.protocol.admin.ReadOnlyFetchDisabledException;
import voldemort.server.protocol.admin.StoreVersionAlreadyExistsException;
import voldemort.store.UnreachableStoreException;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.regex.Pattern;

/**
 * Static utility functions to deal with exceptions.
 */
public class ExceptionUtils {
    /**
     * Inspects a given {@link Throwable} as well as its nested causes, in order to look
     * for a specific set of exception classes. The function also detects if the throwable
     * to inspect is a subclass of one of the classes you look for, but not the other way
     * around (i.e.: if you're looking for the subclass but the throwableToInspect is the
     * parent class, then this function returns false).
     *
     * @return true if a the throwableToInspect corresponds to or is caused by any of the throwableClassesToLookFor
     */
    public static boolean recursiveClassEquals(Throwable throwableToInspect, Class... throwableClassesToLookFor) {
        for (Class clazz: throwableClassesToLookFor) {
            Class classToInspect = throwableToInspect.getClass();
            while (classToInspect != null) {
                if (classToInspect.equals(clazz)) {
                    return true;
                }
                classToInspect = classToInspect.getSuperclass();
            }
        }
        Throwable cause = throwableToInspect.getCause();
        return cause != null && recursiveClassEquals(cause, throwableClassesToLookFor);
    }

    /**
     * These errors are considered "soft errors" because they should not prevent a BnP
     * job from succeeding if they happen on "replication factor - 1" nodes, at most.
     *
     * They are provided here because there are various places in the code where we
     * want to treat these kinds of errors in a special way. Always look at the usages
     * and evaluate the impact carefully before altering this list.
     */
    public static final Class[] BNP_SOFT_ERRORS = {UnreachableStoreException.class,
                                                   IOException.class,
                                                   ReadOnlyFetchDisabledException.class};

    /**
     * @return a String representation of the provided throwable's stacktrace.
     */
    public static String stackTraceToString(Throwable throwable) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        throwable.printStackTrace(pw);
        return sw.toString(); // stack trace as a string
    }

    /**
     * This function verifies if the passed in exception represents an
     * {@link AsyncOperationNotFoundException}, while taking into account that old
     * server implementations did not have a dedicated exception type for this.
     *
     * @param e the exception to inspect
     * @return true if it represents an {@link AsyncOperationNotFoundException}, false otherwise.
     */
    public static boolean isAsyncOpNotFound(Exception e) {
        if (e instanceof AsyncOperationNotFoundException) {
            // Updated servers should throw this.
            return true;
        } else if (e instanceof VoldemortException) {
            // Legacy servers should throw a generic VoldEx identifiable with this regex
            String exceptionMessage = e.getMessage();
            String regex = String.format(AsyncOperationNotFoundException.MESSAGE, ".*");
            return Pattern.matches(regex, exceptionMessage);
        } else {
            // Everything else is not an AsyncOpNotFoundException
            return false;
        }
    }

    /**
     * This function verifies if the passed in exception represents an
     * {@link StoreVersionAlreadyExistsException}, while taking into account that old
     * server implementations did not have a dedicated exception type for this.
     *
     * @param e the exception to inspect
     * @return true if it represents an {@link StoreVersionAlreadyExistsException}, false otherwise.
     */
    public static boolean isStoreVersionAlreadyExists(Exception e) {
        if (e instanceof StoreVersionAlreadyExistsException) {
            // Updated servers should throw this.
            return true;
        } else if (e instanceof VoldemortException) {
            // Legacy servers should throw a generic VoldEx identifiable with this regex
            String exceptionMessage = e.getMessage();
            String regex = String.format(StoreVersionAlreadyExistsException.MESSAGE, ".*");
            return Pattern.matches(regex, exceptionMessage);
        } else {
            // Everything else is not an AsyncOpNotFoundException
            return false;
        }
    }
}
