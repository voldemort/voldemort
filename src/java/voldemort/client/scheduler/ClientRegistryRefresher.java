package voldemort.client.scheduler;

import org.apache.log4j.Logger;

import voldemort.client.ClientInfo;
import voldemort.client.SystemStore;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class ClientRegistryRefresher implements Runnable {

    private final Logger logger = Logger.getLogger(this.getClass());

    private final SystemStore<String, ClientInfo> clientRegistry;
    private ClientInfo clientInfo;
    private final String clientId;
    private Version lastVersion;

    public ClientRegistryRefresher(SystemStore<String, ClientInfo> clientRegistry,
                                   String clientId,
                                   ClientInfo clientInfo,
                                   Version version) {
        this.clientRegistry = clientRegistry;
        this.clientInfo = clientInfo;
        this.clientId = clientId;
        this.lastVersion = version;
        logger.info("Initial version obtained from client registry: " + version);
    }

    public void run() {
        clientInfo.setUpdateTime(System.currentTimeMillis());
        logger.info("updating client registry with the following info for client: " + clientId
                    + "\n" + clientInfo);
        try {
            lastVersion = clientRegistry.putSysStore(clientId,
                                                     new Versioned<ClientInfo>(clientInfo,
                                                                               lastVersion));
        } catch(Exception e) {
            logger.warn("encounted the following error while trying to update client registry: "
                        + e);
        }
    }
}
