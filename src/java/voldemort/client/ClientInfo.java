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
    private long updateTime;
    private String releaseVersion;

    public ClientInfo(String storeName,
                      String clientContext,
                      int clientSequence,
                      long bootstrapTime,
                      String version) {
        this.bootstrapTime = bootstrapTime;
        this.storeName = storeName;
        this.context = clientContext;
        this.sequence = clientSequence;
        this.localHostName = createHostName();
        this.deploymentPath = createDeploymentPath();
        this.updateTime = bootstrapTime;
        this.releaseVersion = version;

        if(logger.isDebugEnabled()) {
            logger.debug(this.toString());
        }
    }

    private synchronized String createDeploymentPath() {
        String currentPath = null;
        try {
            currentPath = new File(".").getCanonicalPath();
        } catch(IOException e) {
            logger.warn("Unable to obtain client deployment path due to the following error:");
            logger.warn(e.getMessage());
        }
        return currentPath;
    }

    private synchronized String createHostName() {
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

    public synchronized void setStoreName(String storeName) {
        this.storeName = storeName;
    }

    public synchronized String getStoreName() {
        return storeName;
    }

    public synchronized void setBootstrapTime(long bootstrapTime) {
        this.bootstrapTime = bootstrapTime;
    }

    public synchronized long getBootstrapTime() {
        return bootstrapTime;
    }

    public synchronized void setContext(String clientContext) {
        this.context = clientContext;
    }

    public synchronized String getContext() {
        return context;
    }

    public synchronized void setClientSequence(int clientSequence) {
        this.sequence = clientSequence;
    }

    public synchronized int getClientSequence() {
        return sequence;
    }

    public synchronized void setDeploymentPath(String deploymentPath) {
        this.deploymentPath = deploymentPath;
    }

    public synchronized String getDeploymentPath() {
        return deploymentPath;
    }

    public synchronized void setLocalHostName(String localHostName) {
        this.localHostName = localHostName;
    }

    public synchronized String getLocalHostName() {
        return localHostName;
    }

    public synchronized void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
    }

    public synchronized long getUpdateTime() {
        return this.updateTime;
    }

    public synchronized void setReleaseVersion(String version) {
        this.releaseVersion = version;
    }

    public synchronized String getReleaseVersion() {
        return this.releaseVersion;
    }

    @Override
    public boolean equals(Object object) {
        if(this == object)
            return true;
        if(object == null)
            return false;
        if(!object.getClass().equals(ClientInfo.class))
            return false;
        ClientInfo clientInfo = (ClientInfo) object;
        return (this.bootstrapTime == clientInfo.bootstrapTime)
               && (this.context.equals(clientInfo.context))
               && (this.deploymentPath.equals(clientInfo.deploymentPath))
               && (this.localHostName.equals(clientInfo.localHostName))
               && (this.sequence == clientInfo.sequence)
               && (this.storeName.equals(clientInfo.storeName))
               && (this.updateTime == clientInfo.updateTime)
               && (this.releaseVersion == clientInfo.releaseVersion);
    }

    @Override
    public synchronized String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("boostrapTime[").append(bootstrapTime).append("], ");
        builder.append("context[").append(context).append("], ");
        builder.append("deploymentPath[").append(deploymentPath).append("], ");
        builder.append("localHostName[").append(localHostName).append("], ");
        builder.append("sequence[").append(sequence).append("], ");
        builder.append("storeName[").append(storeName).append("], ");
        builder.append("updateTime[").append(updateTime).append("], ");
        builder.append("releaseVersion[").append(releaseVersion).append("]");
        return builder.toString();
    }
}
