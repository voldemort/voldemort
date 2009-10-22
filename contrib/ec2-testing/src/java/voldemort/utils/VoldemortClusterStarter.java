package voldemort.utils;

import java.io.File;
import java.util.Collection;

public interface VoldemortClusterStarter {

    public void start(Collection<String> hostNames,
                      String hostUserId,
                      File sshPrivateKey,
                      String voldemortRootDirectory,
                      String voldemortHomeDirectory) throws VoldemortStartClusterException;

}