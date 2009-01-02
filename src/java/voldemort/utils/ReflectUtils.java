package voldemort.utils;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Utilities for reflection
 * 
 * @author jay
 * 
 */
public class ReflectUtils {

    public static <T> T construct(Class<T> klass, Object[] args) {
        Class<?>[] klasses = new Class[args.length];
        for (int i = 0; i < args.length; i++)
            klasses[i] = args[i].getClass();
        try {
            Constructor<T> cons = klass.getConstructor(klasses);
            T t = cons.newInstance(args);
            return t;
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Could not construct " + klass + " with arguments " + args);
        } catch (InvocationTargetException e) {
            throw new RuntimeException("Could not construct " + klass + " with arguments " + args);
        } catch (InstantiationException e) {
            throw new RuntimeException("Could not construct " + klass + " with arguments " + args);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Could not construct " + klass + " with arguments " + args);
        }
    }

    public static String getPropertyName(String name) {
        if (name != null && (name.startsWith("get") || name.startsWith("set"))) {
            StringBuilder b = new StringBuilder(name);
            b.delete(0, 3);
            b.setCharAt(0, Character.toLowerCase(b.charAt(0)));
            return b.toString();
        } else {
            return name;
        }
    }

}
