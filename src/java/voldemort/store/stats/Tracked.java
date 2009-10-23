package voldemort.store.stats;

public enum Tracked {
    GET("get"),
    GET_ALL("getAll"),
    PUT("put"),
    DELETE("delete"),
    EXCEPTION("exception"),
    OBSOLETE("obsolete");

    private final String name;

    private Tracked(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}
