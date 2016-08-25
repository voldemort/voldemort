package voldemort.store.readonly.mr.utils;

import java.util.Properties;
import junit.framework.TestCase;
import voldemort.server.VoldemortConfig;

/**
 * Tests for VoldemortUtils.
 */
public class VoldemortUtilsTest extends TestCase {
    private static final String PATH = "user/testpath";
    public void testModifyURLByConfig() {
        String url = "swebhdfs://localhost:50470/" + PATH;
        Properties properties = new Properties();
        properties.setProperty(VoldemortConfig.NODE_ID, "1");
        properties.setProperty(VoldemortConfig.VOLDEMORT_HOME, "/test");
        VoldemortConfig config = new VoldemortConfig(properties);

        // Default configuration. Should not modify url.
        String newUrl = VoldemortUtils.modifyURL(url, config);
        assertEquals(url, newUrl);

        // Enable modify feature. URL should be replace to webhdfs with 50070 port number.
        properties.setProperty(VoldemortConfig.READONLY_MODIFY_PROTOCOL, "webhdfs");
        properties.setProperty(VoldemortConfig.READONLY_MODIFY_PORT, "50070");
        config = new VoldemortConfig(properties);
        newUrl = VoldemortUtils.modifyURL(url, config);
        assertEquals("webhdfs://localhost:50070/" + PATH, newUrl);

        // No modified protocol assigned. Should not modify URL.
        properties.remove(VoldemortConfig.READONLY_MODIFY_PORT);
        config = new VoldemortConfig(properties);
        newUrl = VoldemortUtils.modifyURL(url, config);
        assertEquals(url, newUrl);

        // No Modified port assigned. Should not modify URL.
        properties.remove(VoldemortConfig.READONLY_MODIFY_PORT);
        properties.setProperty(VoldemortConfig.READONLY_MODIFY_PROTOCOL, "testprotocol");
        config = new VoldemortConfig(properties);
        newUrl = VoldemortUtils.modifyURL(url, config);
        assertEquals(url, newUrl);

        // Omit port set to true should remove the port from the URI
        String expectedUrl = "testprotocol://localhost/" + PATH;
        properties.setProperty(VoldemortConfig.READONLY_MODIFY_PROTOCOL, "testprotocol");
        properties.setProperty(VoldemortConfig.READONLY_OMIT_PORT, "true");
        config = new VoldemortConfig(properties);
        newUrl = VoldemortUtils.modifyURL(url, config);
        assertEquals(expectedUrl, newUrl);

        // Local path. Should throw IAE because it's not a valid URL.
        url = "/testpath/file";
        properties.setProperty(VoldemortConfig.READONLY_MODIFY_PROTOCOL, "webhdfs");
        properties.setProperty(VoldemortConfig.READONLY_MODIFY_PORT, "50070");
        config = new VoldemortConfig(properties);
        try {
          VoldemortUtils.modifyURL(url, config);
        } catch (IllegalArgumentException iae) {
            return;
        } catch (Exception e) {
            fail("Should met IAE. URL is not valid.");
        }
        fail("Should met IAE. URL is not valid.");
    }
}
