package voldemort.utils;

import java.util.Iterator;

/** 
 * An iterator that must be closed after use
 * 
 * @author jay
 *
 * @param <T> The type being iterated over
 */
public interface ClosableIterator<T> extends Iterator<T> {
	
	/**
	 * Close the iterator
	 */
	public void close();

}
