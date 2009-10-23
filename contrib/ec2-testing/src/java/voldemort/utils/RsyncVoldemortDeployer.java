package voldemort.utils;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class RsyncVoldemortDeployer extends CommandLineAction implements VoldemortDeployer {

    public void deploy(Collection<String> hostNames,
                       String hostUserId,
                       File sshPrivateKey,
                       String voldemortRootDirectory,
                       File sourceDirectory) throws VoldemortDeploymentException {
        try {
            List<UnixCommand> unixCommands = generateCommands("RsyncVoldemortDeployer.ssh",
                                                              hostNames,
                                                              hostUserId,
                                                              sshPrivateKey,
                                                              voldemortRootDirectory,
                                                              null,
                                                              sourceDirectory);
            for(UnixCommand unixCommand: unixCommands)
                unixCommand.execute();
        } catch(InterruptedException e) {
            throw new VoldemortDeploymentException(e);
        } catch(IOException e) {
            throw new VoldemortDeploymentException(e);
        }

        try {
            List<UnixCommand> unixCommands = generateCommands("RsyncVoldemortDeployer.rsync",
                                                              hostNames,
                                                              hostUserId,
                                                              sshPrivateKey,
                                                              voldemortRootDirectory,
                                                              null,
                                                              sourceDirectory);
            for(UnixCommand unixCommand: unixCommands)
                unixCommand.execute();
        } catch(InterruptedException e) {
            throw new VoldemortDeploymentException(e);
        } catch(IOException e) {
            throw new VoldemortDeploymentException(e);
        }
    }

}