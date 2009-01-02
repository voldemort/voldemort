package voldemort.annotations.concurrency;

/**
 * Indicates the lock that protects a given variable
 * 
 * @author jay
 *
 */
public @interface LockedBy {
	
	String value();

}
