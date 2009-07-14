package voldemort.socketpool;

import voldemort.utils.socketpool.PoolableObjectFactory;

public class PoolableObjectFactoryFactory {

    public static PoolableObjectFactory<String, String> getBasicPoolFactory() {
        return new PoolableObjectFactory<String, String>() {

            public String activate(String key, String value) throws Exception {
                return value;
            }

            public String create(String key) throws Exception {
                return new String("resource");
            }

            public void destroy(String key, String obj) throws Exception {}

            public String passivate(String key, String value) throws Exception {
                return value;
            }

            public boolean validate(String key, String value) throws Exception {
                return true;
            }
        };
    }
}