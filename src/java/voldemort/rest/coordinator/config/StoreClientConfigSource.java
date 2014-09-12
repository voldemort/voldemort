package voldemort.rest.coordinator.config;

/**
 * The kinds of config sources supported or planned for Coordinator
 */
public enum StoreClientConfigSource {
    FILE,
    ZOOKEEPER;

    public static StoreClientConfigSource get(String value) {
        return StoreClientConfigSource.valueOf(value.trim().toUpperCase());
    }
}
