package voldemort.client;

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.junit.Test;

public class ClientConfigTest {

    @Test
    public void testBootstrapStoresXml() {
        for (boolean b : new boolean[] { true, false }) {
            Properties props = new Properties();
            props.setProperty("fetch_all_stores_xml_in_bootstrap", Boolean.toString(b));
            ClientConfig config = new ClientConfig(props);
            assertEquals("fetch_all_stores_xml mismatch", b, config.isFetchAllStoresXmlInBootstrap());
        }

        for (boolean b : new boolean[] { true, false }) {
            ClientConfig config = new ClientConfig().setFetchAllStoresXmlInBootstrap(b);
            assertEquals("fetch_all_stores_xml mismatch", b, config.isFetchAllStoresXmlInBootstrap());
        }
    }

    @Test
    public void testBootstrapRetryWaitTime() {
        for (int val : new int[] { 0, 1, 2, 3, 4, 5 }) {
            Properties props = new Properties();
            props.setProperty("bootstrap_retry_wait_time_seconds", Integer.toString(val));
            try {
            ClientConfig config = new ClientConfig(props);
            assertEquals("bootstrap retry wait time seconds", val, config.getBootstrapRetryWaitTimeSeconds());
            } catch (IllegalArgumentException ex) {
                assertEquals("bootstrap fails for positive number", 0, val);
            }
        }

        for (int val : new int[] { 0, 1, 2, 3, 4, 5 }) {
            try {
                ClientConfig config = new ClientConfig().setBootstrapRetryWaitTimeSeconds(val);
                assertEquals("bootstrap retry wait time seconds", val, config.getBootstrapRetryWaitTimeSeconds());
            } catch (IllegalArgumentException ex) {
                assertEquals("bootstrap fails for positive number", 0, val);
            }
        }
    }
}
