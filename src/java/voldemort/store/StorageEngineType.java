package voldemort.store;

/**
 * Enum constant for various Storage Engine types
 * 
 * @author jay
 * 
 */
public enum StorageEngineType {

    /**
     * An engine based on Oracle BDB, the default
     */
    BDB("bdb"),

    /**
     * An engine based on MySQL
     */
    MYSQL("mysql"),

    /**
     * A non-persistent store that uses a ConcurrentHashMap to store data.
     */
    MEMORY("memory"),

    /**
     * A non-persistent store that uses a ConcurrentMap and stores data with
     * soft references; this means that data will be garbage collected when the
     * GC is under memory pressure
     */
    CACHE("cache"),

    /**
     * A filesystem based store that uses a single file for each value
     */
    FILESYSTEM("fs"),

    /**
     * A readonly, file-system store that keeps all data in a large sorted file
     */
    READONLY("read-only");

    private final String text;

    private StorageEngineType(String text) {
        this.text = text;
    }

    public static StorageEngineType fromDisplay(String type) {
        for(StorageEngineType t: StorageEngineType.values())
            if(t.toDisplay().equals(type))
                return t;
        throw new IllegalArgumentException("No StoreType " + type + " exists.");
    }

    public String toDisplay() {
        return text;
    }
};
