package voldemort.utils;

import java.io.File;
import java.util.Collection;

public interface VoldemortDeployer {

    public void deploy(Collection<String> hostNames,
                       String hostUserId,
                       File sshPrivateKey,
                       File sourceDirectory,
                       String clusterXml,
                       String storesXml,
                       String serverProperties,
                       String destinationDirectory) throws VoldemortDeploymentException;

}
