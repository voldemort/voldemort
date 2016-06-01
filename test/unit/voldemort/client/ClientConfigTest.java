package voldemort.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

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

    @Test
    public void testIdleConnectionTimeout() {
        String idleConnectionTimeoutProperty = "idle_connection_timeout_minutes";

        // Disabled test cases
        ClientConfig config1 = new ClientConfig().setIdleConnectionTimeout(0, TimeUnit.NANOSECONDS);
        assertEquals("setting to 0, should return -1, regardless of the unit",
                     -1,
                     config1.getIdleConnectionTimeout(TimeUnit.DAYS));

        Properties props1 = new Properties();
        props1.setProperty("idle_connection_timeout_minutes", "-1");
        ClientConfig config2 = new ClientConfig(props1);
        assertEquals("setting to 0, should return -1, regardless of the unit",
                     -1,
                     config2.getIdleConnectionTimeout(TimeUnit.DAYS));

        // Negative test cases, where it should throw
        for (int i = 1; i < 10; i++) {
            for (TimeUnit unit : new TimeUnit[] { TimeUnit.MINUTES, TimeUnit.SECONDS }) {
                try {
                    ClientConfig config = new ClientConfig().setIdleConnectionTimeout(i, unit);
                    fail("Any thing with less than 10 minute should throw" + i);
                } catch (IllegalArgumentException e) {
                    // expected
                }
            }
        }

        for (int i = 1; i < 10; i++) {
            Properties props = new Properties();
            props.setProperty("idle_connection_timeout_minutes", Integer.toString(i));

            try {
                ClientConfig config = new ClientConfig(props);
                fail("Any thing with less than 10 minute should throw");
            } catch (IllegalArgumentException e) {
                // expected
            }
        }

        // Positive test cases
        for (int i = 10; i < 100; i++) {
            for (TimeUnit unit : new TimeUnit[] { TimeUnit.MINUTES, TimeUnit.DAYS }) {
                ClientConfig config = new ClientConfig().setIdleConnectionTimeout(i, unit);
                assertEquals(" set valud is not same as retrieved", i, config.getIdleConnectionTimeout(unit));
            }
        }
        
        for (int i = 10; i < 100; i++) {
            Properties props = new Properties();
            props.setProperty("idle_connection_timeout_minutes", Integer.toString(i));
            
            ClientConfig config = new ClientConfig(props);
            assertEquals(" set valud is not same as retrieved", i, config.getIdleConnectionTimeout(TimeUnit.MINUTES));
        }
    }
}
