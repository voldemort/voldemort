package voldemort.client;

public class SystemStoreClientFactory extends SocketStoreClientFactory {

    public SystemStoreClientFactory(ClientConfig config) {
        super(config);
    }

    @Override
    public int getNextJmxId() {
        // for system store, we don't increment jmx id
        return getCurrentJmxId();
    }
}
