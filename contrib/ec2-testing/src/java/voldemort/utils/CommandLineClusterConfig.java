package voldemort.utils;

import java.io.File;
import java.util.Collection;
import java.util.Map;

public class CommandLineClusterConfig {

    private Collection<String> hostNames;

    private String hostUserId;

    private File sshPrivateKey;

    private String voldemortParentDirectory;

    private String voldemortRootDirectory;

    private String voldemortHomeDirectory;

    private Map<String, Integer> nodeIds;

    private File sourceDirectory;

    private Map<String, String> remoteTestArguments;

    public Collection<String> getHostNames() {
        return hostNames;
    }

    public void setHostNames(Collection<String> hostNames) {
        this.hostNames = hostNames;
    }

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

    public String getVoldemortParentDirectory() {
        return voldemortParentDirectory;
    }

    public void setVoldemortParentDirectory(String voldemortParentDirectory) {
        this.voldemortParentDirectory = voldemortParentDirectory;
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

    public Map<String, Integer> getNodeIds() {
        return nodeIds;
    }

    public void setNodeIds(Map<String, Integer> nodeIds) {
        this.nodeIds = nodeIds;
    }

    public File getSourceDirectory() {
        return sourceDirectory;
    }

    public void setSourceDirectory(File sourceDirectory) {
        this.sourceDirectory = sourceDirectory;
    }

    public Map<String, String> getRemoteTestArguments() {
        return remoteTestArguments;
    }

    public void setRemoteTestArguments(Map<String, String> remoteTestArguments) {
        this.remoteTestArguments = remoteTestArguments;
    }

}
