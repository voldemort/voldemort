package voldemort.versioning;

import java.util.List;

import voldemort.VoldemortException;

/**
 * Thrown when multiple inconsistent values are found and no resolution strategy
 * resolves the problem
 * 
 * @author jay
 * 
 */
public class InconsistentDataException extends VoldemortException {

    private static final long serialVersionUID = 1L;

    private List<?> versions;

    public InconsistentDataException(String message, List<?> versions) {
        super(message);
    }

    public short getId() {
        return 8;
    }

    public List<?> getVersions() {
        return this.versions;
    }

}
