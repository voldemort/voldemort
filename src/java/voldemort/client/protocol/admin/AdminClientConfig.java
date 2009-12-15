package voldemort.client.protocol.admin;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import voldemort.client.ClientConfig;
import voldemort.utils.Props;

/**
 * Client Configuration properties for Admin client. extends
 * {@link ClientConfig}<br>
 * Sets better <b><i>default</i></b> values for properties used by
 * {@link AdminClient}.
 * 
 * @author bbansal
 * 
 */
public class AdminClientConfig extends ClientConfig {

    // sets better default for AdminClient
    public AdminClientConfig() {
        super(new Properties());

    }

    public AdminClientConfig(Properties properties) {
        super(properties);

        Props props = new Props(properties);
        // set better defaults for AdminClient

        if(!props.containsKey(ClientConfig.CONNECTION_TIMEOUT_MS_PROPERTY))
            this.setConnectionTimeout(60, TimeUnit.SECONDS);

        if(!props.containsKey(ClientConfig.SOCKET_TIMEOUT_MS_PROPERTY))
            this.setSocketTimeout(24 * 60 * 60, TimeUnit.SECONDS);

        if(!props.containsKey(ClientConfig.SOCKET_BUFFER_SIZE_PROPERTY))
            this.setSocketBufferSize(16 * 1024 * 1024);

        if(!props.containsKey(ClientConfig.MAX_CONNECTIONS_PER_NODE_PROPERTY))
            this.setMaxConnectionsPerNode(3);

        if(!props.containsKey(ClientConfig.MAX_THREADS_PROPERTY))
            this.setMaxThreads(8);
    }
}
