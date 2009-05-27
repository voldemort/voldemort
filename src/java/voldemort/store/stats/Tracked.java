package voldemort.store.stats;

public enum Tracked {
    GET("Get"),
    GET_ALL("Get All"),
    PUT("Put"),
    DELETE("Delete"),
    EXCEPTION("Exception");

    private final String name;

    private Tracked(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}
