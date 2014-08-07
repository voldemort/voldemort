package voldemort.socketpool;

import voldemort.client.protocol.admin.SocketAndStreams;
import voldemort.client.protocol.admin.SocketResourceFactory;
import voldemort.store.socket.SocketDestination;
import voldemort.utils.pool.KeyedResourcePool;
import voldemort.utils.pool.ResourceFactory;

public class ResourcePoolTestUtils {

    public static ResourceFactory<String, String> getBasicPoolFactory() {
        return new ResourceFactory<String, String>() {

            @Override
            public void createAsync(String key, KeyedResourcePool<String, String> pool)
                    throws Exception {
                pool.checkin(key, "resource");
            }

            public void destroy(String key, String obj) throws Exception {}

            public boolean validate(String key, String value) {
                return true;
            }

            public void close() {}

        };
    }

    public static ResourceFactory<SocketDestination, SocketAndStreams> getSocketPoolFactory() {
        return new SocketResourceFactory(100, 1000);
    }

}