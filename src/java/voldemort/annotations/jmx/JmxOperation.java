package voldemort.annotations.jmx;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import javax.management.MBeanOperationInfo;

/**
 * Mark the given method as accessible from JMX
 * 
 * @author jay
 * 
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface JmxOperation {

    public String description() default "";

    public int impact() default MBeanOperationInfo.UNKNOWN;
}
