package voldemort.socketpool;

import voldemort.store.socket.SocketAndStreams;
import voldemort.store.socket.SocketDestination;
import voldemort.store.socket.SocketResourceFactory;
import voldemort.utils.pool.ResourceFactory;

public class ResourcePoolTestUtils {

    public static ResourceFactory<String, String> getBasicPoolFactory() {
        return new ResourceFactory<String, String>() {

            public String create(String key) throws Exception {
                return "resource";
            }

            public void destroy(String key, String obj) throws Exception {}

            public boolean validate(String key, String value) {
                return true;
            }
        };
    }

    public static ResourceFactory<SocketDestination, SocketAndStreams> getSocketPoolFactory() {
        return new SocketResourceFactory(100, 1000) {};
    }
}