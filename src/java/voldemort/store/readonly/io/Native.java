package voldemort.store.readonly.io;

import java.io.FileDescriptor;
import java.lang.reflect.Field;

import org.apache.log4j.Logger;

public class Native {

    private static final Logger log = Logger.getLogger(Native.class);

    /**
     * Used to get access to protected/private field of the specified class
     * 
     * @param klass - name of the class
     * @param fieldName - name of the field
     * @return Field or null on error
     */
    @SuppressWarnings("rawtypes")
    public static Field getProtectedField(Class className, String fieldName) {

        Field field;

        try {
            field = className.getDeclaredField(fieldName);
            field.setAccessible(true);
        } catch(Exception e) {
            throw new AssertionError(e);
        }

        return field;
    }

    public static int getFd(FileDescriptor descriptor) {

        Field field = getProtectedField(descriptor.getClass(), "fd");

        if(field == null)
            return -1;

        try {
            return field.getInt(descriptor);
        } catch(Exception e) {
            log.warn("unable to read fd field from FileDescriptor");
        }

        return -1;

    }

}
