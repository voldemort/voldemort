package voldemort.restclient;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;

import voldemort.client.ClientConfig;
import voldemort.client.TimeoutConfig;
import voldemort.common.VoldemortOpCode;
import voldemort.utils.ConfigurationException;
import voldemort.utils.Props;

/**
 * Maintains the config parameters for the REST client
 * 
 */
public class RESTClientConfig {

    private String httpBootstrapURL = null;
    private int maxR2ConnectionPoolSize = 100;
    private long timeoutMs = 5000;
    private TimeoutConfig timeoutConfig = new TimeoutConfig(timeoutMs, false);
    private boolean enableJmx = true;

    /**
     * Instantiate the RESTClient config using a properties file
     * 
     * @param propertyFile Properties file
     */
    public RESTClientConfig(File propertyFile) {
        Properties properties = new Properties();
        InputStream input = null;
        try {
            input = new BufferedInputStream(new FileInputStream(propertyFile.getAbsolutePath()));
            properties.load(input);
        } catch(IOException e) {
            throw new ConfigurationException(e);
        } finally {
            IOUtils.closeQuietly(input);
        }
        setProperties(properties);
    }

    /**
     * Initiate the RESTClient config from a set of properties. This is useful
     * for wiring from Spring or for externalizing client properties to a
     * properties file
     * 
     * @param properties The properties to use
     */
    public RESTClientConfig(Properties properties) {
        setProperties(properties);
    }

    /**
     * Dummy constructor for testing purposes
     */
    public RESTClientConfig() {}

    /**
     * Set the values using the specified Properties object.
     * 
     * @param properties Properties object containing specific property values
     *        for the RESTClient config
     * 
     *        Note: We're using the same property names as that in ClientConfig
     *        for backwards compatibility.
     */
    private void setProperties(Properties properties) {
        Props props = new Props(properties);
        if(props.containsKey(ClientConfig.ENABLE_JMX_PROPERTY)) {
            this.setEnableJmx(props.getBoolean(ClientConfig.ENABLE_JMX_PROPERTY));
        }

        if(props.containsKey(ClientConfig.BOOTSTRAP_URLS_PROPERTY)) {
            List<String> urls = props.getList(ClientConfig.BOOTSTRAP_URLS_PROPERTY);
            if(urls.size() > 0) {
                setHttpBootstrapURL(urls.get(0));
            }
        }

        if(props.containsKey(ClientConfig.MAX_TOTAL_CONNECTIONS_PROPERTY)) {
            setMaxR2ConnectionPoolSize(props.getInt(ClientConfig.MAX_TOTAL_CONNECTIONS_PROPERTY,
                                                    maxR2ConnectionPoolSize));
        }

        if(props.containsKey(ClientConfig.ROUTING_TIMEOUT_MS_PROPERTY))
            this.setTimeoutMs(props.getLong(ClientConfig.ROUTING_TIMEOUT_MS_PROPERTY, timeoutMs),
                              TimeUnit.MILLISECONDS);

        // By default, make all the timeouts equal to routing timeout
        timeoutConfig = new TimeoutConfig(timeoutMs, false);

        if(props.containsKey(ClientConfig.GETALL_ROUTING_TIMEOUT_MS_PROPERTY))
            timeoutConfig.setOperationTimeout(VoldemortOpCode.GET_ALL_OP_CODE,
                                              props.getInt(ClientConfig.GETALL_ROUTING_TIMEOUT_MS_PROPERTY));

        if(props.containsKey(ClientConfig.GET_ROUTING_TIMEOUT_MS_PROPERTY))
            timeoutConfig.setOperationTimeout(VoldemortOpCode.GET_OP_CODE,
                                              props.getInt(ClientConfig.GET_ROUTING_TIMEOUT_MS_PROPERTY));

        if(props.containsKey(ClientConfig.PUT_ROUTING_TIMEOUT_MS_PROPERTY)) {
            long putTimeoutMs = props.getInt(ClientConfig.PUT_ROUTING_TIMEOUT_MS_PROPERTY);
            timeoutConfig.setOperationTimeout(VoldemortOpCode.PUT_OP_CODE, putTimeoutMs);
            // By default, use the same thing for getVersions() also
            timeoutConfig.setOperationTimeout(VoldemortOpCode.GET_VERSION_OP_CODE, putTimeoutMs);
        }

        // of course, if someone overrides it, we will respect that
        if(props.containsKey(ClientConfig.GET_VERSIONS_ROUTING_TIMEOUT_MS_PROPERTY))
            timeoutConfig.setOperationTimeout(VoldemortOpCode.GET_VERSION_OP_CODE,
                                              props.getInt(ClientConfig.GET_VERSIONS_ROUTING_TIMEOUT_MS_PROPERTY));

        if(props.containsKey(ClientConfig.DELETE_ROUTING_TIMEOUT_MS_PROPERTY))
            timeoutConfig.setOperationTimeout(VoldemortOpCode.DELETE_OP_CODE,
                                              props.getInt(ClientConfig.DELETE_ROUTING_TIMEOUT_MS_PROPERTY));

        if(props.containsKey(ClientConfig.ALLOW_PARTIAL_GETALLS_PROPERTY))
            timeoutConfig.setPartialGetAllAllowed(props.getBoolean(ClientConfig.ALLOW_PARTIAL_GETALLS_PROPERTY));

    }

    public String getHttpBootstrapURL() {
        return httpBootstrapURL;
    }

    /**
     * Specifies the HTTP Bootstrap URL for this client to contact
     * 
     * <ul>
     * <li>Property : "bootstrap_urls"</li>
     * <li>Default : None</li>
     * </ul>
     * 
     */
    public RESTClientConfig setHttpBootstrapURL(String httpBootstrapURL) {
        this.httpBootstrapURL = httpBootstrapURL;
        return this;
    }

    public int getMaxR2ConnectionPoolSize() {
        return maxR2ConnectionPoolSize;
    }

    /**
     * Specifies the size of the R2Store connection pool. This includes
     * connections for all the nodes in the cluster.
     * 
     * <ul>
     * <li>Property : "max_total_connections"</li>
     * <li>Default : 100</li>
     * </ul>
     * 
     */
    public RESTClientConfig setMaxR2ConnectionPoolSize(int maxR2PoolSize) {
        this.maxR2ConnectionPoolSize = maxR2PoolSize;
        return this;
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }

    /**
     * Specifies the Global timeout for the different client operations
     * 
     * <ul>
     * <li>Property : "routing_timeout_ms"</li>
     * <li>Default : 5000</li>
     * </ul>
     * 
     */
    public RESTClientConfig setTimeoutMs(long timeoutMs, TimeUnit unit) {
        this.timeoutMs = unit.toMillis(timeoutMs);
        return this;
    }

    public TimeoutConfig getTimeoutConfig() {
        return timeoutConfig;
    }

    public boolean isEnableJmx() {
        return enableJmx;
    }

    /**
     * Determines if JMX monitoring is to be enabled
     * 
     * <ul>
     * <li>Property : "enable_jmx"</li>
     * <li>Default : TRUE</li>
     * </ul>
     * 
     */
    public void setEnableJmx(boolean enableJmx) {
        this.enableJmx = enableJmx;
    }
}