package voldemort.versioning;

/**
 * An interface that allows us to determine if a given version happened before or
 * after another version.
 * 
 * This could have been done using the comparable interface but that is confusing,
 * because the numeric codes are easily confused, and because concurrent versions are 
 * not necessarily "equal" in the normal sense.
 * 
 * @author jay
 *
 */
public interface Version {
	
	/**
	 * Return whether or not the given version preceeded this one, succeeded it, or is concurrant with it
	 * @param v The other version
	 */
	public Occured compare(Version v);
	
}
