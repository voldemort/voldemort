package voldemort.client;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;

public class ClientInfo implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    protected static final Logger logger = Logger.getLogger(ClientInfo.class);

    private long bootstrapTime;
    private String storeName;
    private String context;
    private int sequence;
    private String localHostName;
    private String deploymentPath;

    public ClientInfo(String storeName, String clientContext, int clientSequence, long bootstrapTime) {
        this.bootstrapTime = bootstrapTime;
        this.storeName = storeName;
        this.context = clientContext;
        this.sequence = clientSequence;
        this.localHostName = createHostName();
        this.deploymentPath = createDeploymentPath();
    }

    private String createDeploymentPath() {
        String currentPath = null;
        try {
            currentPath = new File(".").getCanonicalPath();
        } catch(IOException e) {
            logger.warn("Unable to obtain client deployment path due to the following error:");
            logger.warn(e.getMessage());
        }
        return currentPath;
    }

    private String createHostName() {
        String hostName = null;
        try {
            InetAddress host = InetAddress.getLocalHost();
            hostName = host.getHostName();
        } catch(UnknownHostException e) {
            logger.warn("Unable to obtain client hostname due to the following error:");
            logger.warn(e.getMessage());
        }
        return hostName;
    }

    public void setStoreName(String storeName) {
        this.storeName = storeName;
    }

    public String getStoreName() {
        return storeName;
    }

    public void setBootstrapTime(long bootstrapTime) {
        this.bootstrapTime = bootstrapTime;
    }

    public long getBootstrapTime() {
        return bootstrapTime;
    }

    public void setContext(String clientContext) {
        this.context = clientContext;
    }

    public String getContext() {
        return context;
    }

    public void setClientSequence(int clientSequence) {
        this.sequence = clientSequence;
    }

    public int getClientSequence() {
        return sequence;
    }

    public void setDeploymentPath(String deploymentPath) {
        this.deploymentPath = deploymentPath;
    }

    public String getDeploymentPath() {
        return deploymentPath;
    }

    public void setLocalHostName(String localHostName) {
        this.localHostName = localHostName;
    }

    public String getLocalHostName() {
        return localHostName;
    }
}
