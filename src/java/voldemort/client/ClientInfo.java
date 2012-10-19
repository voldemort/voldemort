/*
 * Copyright 2008-2012 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.client;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

/**
 * A collection of voldemort client side information what will be populated into
 * the voldemort cluster when a client is connected to a voldemort cluster
 * 
 */
public class ClientInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    protected static final Logger logger = Logger.getLogger(ClientInfo.class);

    private long bootstrapTimestampMs;
    private String storeName;
    private String context;
    private int sequence;
    private String localHostName;
    private String deploymentPath;
    private long updateTimestampMs;
    private String releaseVersion;
    private ClientConfig config;
    private long clusterMetadataVersion;

    public ClientInfo(String storeName,
                      String clientContext,
                      int clientSequence,
                      long bootstrapTime,
                      String version,
                      ClientConfig config) {
        this.bootstrapTimestampMs = bootstrapTime;
        this.storeName = storeName;
        this.context = clientContext;
        this.sequence = clientSequence;
        this.localHostName = createHostName();
        this.deploymentPath = createDeploymentPath();
        this.updateTimestampMs = bootstrapTime;
        this.releaseVersion = version;
        this.config = config;
        this.clusterMetadataVersion = 0;

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
        this.bootstrapTimestampMs = bootstrapTime;
    }

    public synchronized long getBootstrapTime() {
        return bootstrapTimestampMs;
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
        this.updateTimestampMs = updateTime;
    }

    public synchronized long getUpdateTime() {
        return this.updateTimestampMs;
    }

    public synchronized void setReleaseVersion(String version) {
        this.releaseVersion = version;
    }

    public synchronized String getReleaseVersion() {
        return this.releaseVersion;
    }

    public synchronized ClientConfig getClientConfig() {
        return this.config;
    }

    public synchronized void setClusterMetadataVersion(long newVersion) {
        this.clusterMetadataVersion = newVersion;
    }

    /**
     * At the moment we're not checking if the Config objects are similar. TODO:
     * reevaluate in the future.
     */
    @Override
    public boolean equals(Object object) {
        if(this == object)
            return true;
        if(object == null)
            return false;
        if(!object.getClass().equals(ClientInfo.class))
            return false;
        ClientInfo clientInfo = (ClientInfo) object;
        return (this.bootstrapTimestampMs == clientInfo.bootstrapTimestampMs)
               && (this.context.equals(clientInfo.context))
               && (this.deploymentPath.equals(clientInfo.deploymentPath))
               && (this.localHostName.equals(clientInfo.localHostName))
               && (this.sequence == clientInfo.sequence)
               && (this.storeName.equals(clientInfo.storeName))
               && (this.updateTimestampMs == clientInfo.updateTimestampMs)
               && (this.releaseVersion == clientInfo.releaseVersion);
    }

    @Override
    public synchronized String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("bootstrapTime=").append(bootstrapTimestampMs).append("\n");
        builder.append("context=").append(context).append("\n");
        builder.append("deploymentPath=").append(deploymentPath).append("\n");
        builder.append("localHostName=").append(localHostName).append("\n");
        builder.append("sequence=").append(sequence).append("\n");
        builder.append("storeName=").append(storeName).append("\n");
        builder.append("updateTime=").append(updateTimestampMs).append("\n");
        builder.append("releaseVersion=").append(releaseVersion).append("\n");
        builder.append("clusterMetadataVersion=").append(clusterMetadataVersion).append("\n");

        /**
         * Append the Client Config information. Right now we only track the
         * following fields max_connections, max_total_connections,
         * connection_timeout_ms, socket_timeout_ms, routing_timeout_ms,
         * client_zone_id, failuredetector_implementation
         * 
         */
        builder.append("max_connections=")
               .append(this.config.getMaxConnectionsPerNode())
               .append("\n");
        builder.append("max_total_connections=")
               .append(this.config.getMaxTotalConnections())
               .append("\n");
        builder.append("connection_timeout_ms=")
               .append(this.config.getConnectionTimeout(TimeUnit.MILLISECONDS))
               .append("\n");
        builder.append("socket_timeout_ms=")
               .append(this.config.getSocketTimeout(TimeUnit.MILLISECONDS))
               .append("\n");
        builder.append("routing_timeout_ms=")
               .append(this.config.getRoutingTimeout(TimeUnit.MILLISECONDS))
               .append("\n");
        builder.append("client_zone_id=").append(this.config.getClientZoneId()).append("\n");
        builder.append("failuredetector_implementation=")
               .append(this.config.getFailureDetectorImplementation())
               .append("\n");
        builder.append("failuredetector_threshold=")
               .append(this.config.getFailureDetectorThreshold())
               .append("\n");
        builder.append("failuredetector_threshold_count_minimum=")
               .append(this.config.getFailureDetectorThresholdCountMinimum())
               .append("\n");
        builder.append("failuredetector_threshold_interval=")
               .append(this.config.getFailureDetectorThresholdInterval())
               .append("\n");
        builder.append("failuredetector_threshold_async_recovery_interval=")
               .append(this.config.getFailureDetectorAsyncRecoveryInterval())
               .append("\n");

        return builder.toString();
    }
}
