package voldemort.restclient;

import java.util.Properties;

import com.linkedin.r2.transport.common.Client;

public class RESTClientFactoryConfig {

    private Properties clientConfig;
    private Client d2Client;

    public RESTClientFactoryConfig() {}

    public RESTClientFactoryConfig(Properties clientConfig, Client d2Client) {
        setClientConfig(clientConfig);
        setD2Client(d2Client);
    }

    public Properties getClientConfig() {
        return clientConfig;
    }

    public void setClientConfig(Properties clientConfig) {
        this.clientConfig = clientConfig;
    }

    public Client getD2Client() {
        return d2Client;
    }

    public void setD2Client(Client d2Client) {
        this.d2Client = d2Client;
    }
}
