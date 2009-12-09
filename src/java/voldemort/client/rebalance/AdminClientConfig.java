package voldemort.client.rebalance;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import voldemort.client.ClientConfig;
import voldemort.utils.Props;

/**
 * Client Configuration properties for Admin client. extends
 * {@link ClientConfig}
 * 
 * @author bbansal
 * 
 */
public class AdminClientConfig extends ClientConfig {

    // sets better default for AdminClient
    public AdminClientConfig() {
        super();
        this.setConnectionTimeout(60, TimeUnit.SECONDS);
        this.setSocketTimeout(24 * 60 * 60, TimeUnit.SECONDS);
        this.setSocketBufferSize(16 * 1024 * 1024);
        this.setMaxConnectionsPerNode(2);
        this.setMaxThreads(4);
    }

    public AdminClientConfig(Properties properties) {
        super(properties);
        Props props = new Props(properties);
    }
}
