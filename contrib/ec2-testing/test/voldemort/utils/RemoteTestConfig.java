/*
 * Copyright 2009 LinkedIn, Inc.
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

package voldemort.utils;

import java.io.File;

public class RemoteTestConfig {

    protected String hostUserId;

    protected File sshPrivateKey;

    protected String voldemortRootDirectory;

    protected String voldemortHomeDirectory;

    protected File sourceDirectory;

    protected String parentDirectory;

    protected File clusterXmlFile;

    public String getHostUserId() {
        return hostUserId;
    }

    public void setHostUserId(String hostUserId) {
        this.hostUserId = hostUserId;
    }

    public File getSshPrivateKey() {
        return sshPrivateKey;
    }

    public void setSshPrivateKey(File sshPrivateKey) {
        this.sshPrivateKey = sshPrivateKey;
    }

    public String getVoldemortRootDirectory() {
        return voldemortRootDirectory;
    }

    public void setVoldemortRootDirectory(String voldemortRootDirectory) {
        this.voldemortRootDirectory = voldemortRootDirectory;
    }

    public String getVoldemortHomeDirectory() {
        return voldemortHomeDirectory;
    }

    public void setVoldemortHomeDirectory(String voldemortHomeDirectory) {
        this.voldemortHomeDirectory = voldemortHomeDirectory;
    }

    public File getSourceDirectory() {
        return sourceDirectory;
    }

    public void setSourceDirectory(File sourceDirectory) {
        this.sourceDirectory = sourceDirectory;
    }

    public String getParentDirectory() {
        return parentDirectory;
    }

    public void setParentDirectory(String parentDirectory) {
        this.parentDirectory = parentDirectory;
    }

    public File getClusterXmlFile() {
        return clusterXmlFile;
    }

    public void setClusterXmlFile(File clusterXmlFile) {
        this.clusterXmlFile = clusterXmlFile;
    }

}
