package voldemort.utils;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class SshVoldemortClusterStopper extends CommandLineAction implements
        VoldemortClusterStopper {

    public void stop(Collection<String> hostNames,
                     String hostUserId,
                     File sshPrivateKey,
                     String voldemortRootDirectory) throws VoldemortStartClusterException {
        try {
            List<UnixCommand> unixCommands = generateCommands("SshVoldemortClusterStopper.ssh",
                                                              hostNames,
                                                              hostUserId,
                                                              sshPrivateKey,
                                                              voldemortRootDirectory,
                                                              null,
                                                              null);
            for(UnixCommand unixCommand: unixCommands)
                unixCommand.execute();
        } catch(InterruptedException e) {
            throw new VoldemortStartClusterException(e);
        } catch(IOException e) {
            throw new VoldemortStartClusterException(e);
        }
    }

}
