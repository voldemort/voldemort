package voldemort.store;

/**
 * An exception that indicates an attempt by the user to overwrite a newer key
 * with an older key
 * 
 * @author jay
 * 
 */
public class ObsoleteVersionException extends StoreOperationFailureException {

    private static final long serialVersionUID = 1L;

    public ObsoleteVersionException(String arg0) {
        super(arg0);
    }

    public short getId() {
        return 4;
    }

}
