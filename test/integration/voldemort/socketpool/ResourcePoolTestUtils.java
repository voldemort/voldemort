package voldemort.socketpool;

import voldemort.utils.pool.ResourceFactory;

public class ResourcePoolTestUtils {

    public static ResourceFactory<String, String> getBasicPoolFactory() {
        return new ResourceFactory<String, String>() {

            public String create(String key) throws Exception {
                return new String("resource");
            }

            public void destroy(String key, String obj) throws Exception {}

            public boolean validate(String key, String value) {
                return true;
            }
        };
    }
}