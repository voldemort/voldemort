package voldemort.versioning;

/**
 * The result of comparing two times--either t1 is BEFORE t2, t1 is AFTER t2, or
 * t1 happens CONCURRENTLY to t2.
 * 
 * @author jay
 * 
 */
public enum Occured {
    BEFORE,
    AFTER,
    CONCURRENTLY
}
