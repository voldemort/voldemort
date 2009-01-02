package voldemort.versioning;

/**
 * A method for merging two objects
 * 
 * @author jay
 * 
 */
public interface ObjectMerger<T> {

    public T merge(T t1, T t2);

}
