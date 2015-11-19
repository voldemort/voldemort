package voldemort.store.readonly.mr.utils;

import java.util.Properties;
import junit.framework.TestCase;
import voldemort.server.VoldemortConfig;
import voldemort.store.readonly.fetcher.HdfsFetcher;


/**
 * Tests for VoldemortUtils.
 */
public class VoldemortUtilsTest extends TestCase {
    public void testModifyURLByConfig() {
        String url = "swebhdfs://localhost:50470/user/testpath";
        Properties properties = new Properties();
        properties.setProperty("node.id", "1");
        properties.setProperty("voldemort.home", "/test");
        VoldemortConfig config = new VoldemortConfig(properties);

        //Default configuration. Should not modify url.
        HdfsFetcher fetcher = new HdfsFetcher(config);
        String newUrl = VoldemortUtils.modifyURL(url, config.getModifiedProtocol(), config.getMysqlPort());
        assertEquals(url, newUrl);

        //Enable modify feature. URL should be replace to webhdfs with 50070 port number.
        properties.setProperty("readonly.modify.protocol", "webhdfs");
        properties.setProperty("readonly.modify.port", "50070");
        config = new VoldemortConfig(properties);
        newUrl = VoldemortUtils.modifyURL(url, config.getModifiedProtocol(), config.getModifiedPort());
        assertEquals("webhdfs://localhost:50070/user/testpath", newUrl);

        //No modified protocol assigned. Should not modify URL.
        properties.remove("readonly.modify.protocol");
        config = new VoldemortConfig(properties);
        newUrl = VoldemortUtils.modifyURL(url, config.getModifiedProtocol(), config.getModifiedPort());
        assertEquals(url, newUrl);

        //No Modified port assigned. Should not modify URL.
        properties.remove("readonly.modify.port");
        properties.setProperty("readonly.modify.protocol", "testprotocol");
        config = new VoldemortConfig(properties);
        newUrl = VoldemortUtils.modifyURL(url, config.getModifiedProtocol(), config.getModifiedPort());
        assertEquals(url, newUrl);

        //Local path. Should throw IAE because it's not a valid URL.
        url = "/testpath/file";
        properties.setProperty("readonly.modify.protocol", "webhdfs");
        properties.setProperty("readonly.modify.port", "50070");
        config = new VoldemortConfig(properties);
        try {
            VoldemortUtils.modifyURL(url, config.getModifiedProtocol(), config.getModifiedPort());
        } catch (IllegalArgumentException iae) {
            return;
        } catch (Exception e) {
            fail("Should met IAE. URL is not valid.");
        }
        fail("Should met IAE. URL is not valid.");
    }
}
