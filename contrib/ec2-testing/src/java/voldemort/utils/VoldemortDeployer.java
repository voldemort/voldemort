package voldemort.utils;

import java.io.File;
import java.util.Collection;

public interface VoldemortDeployer {

    public void deploy(Collection<String> hostNames,
                       String hostUserId,
                       File sshPrivateKey,
                       String voldemortRootDirectory,
                       File sourceDirectory) throws VoldemortDeploymentException;

}
