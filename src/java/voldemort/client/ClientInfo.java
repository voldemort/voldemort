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

import org.apache.log4j.Logger;

/**
 * A collection of voldemort client side information what will be populated into
 * the voldemort cluster when a client is connected to a voldemort cluster
 * 
 */
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

    public void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
    }

    public long getUpdateTime() {
        return this.updateTime;
    }

    public void setReleaseVersion(String version) {
        this.releaseVersion = version;
    }

    public String getReleaseVersion() {
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
    public String toString() {
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
