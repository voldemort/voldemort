package voldemort.utils;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class SshVoldemortClusterStarter extends CommandLineAction implements
        VoldemortClusterStarter {

    public void start(Collection<String> hostNames,
                      String hostUserId,
                      File sshPrivateKey,
                      String voldemortRootDirectory,
                      String voldemortHomeDirectory) throws VoldemortStartClusterException {
        try {
            List<UnixCommand> unixCommands = generateCommands("SshVoldemortClusterStarter.ssh",
                                                              hostNames,
                                                              hostUserId,
                                                              sshPrivateKey,
                                                              voldemortRootDirectory,
                                                              voldemortHomeDirectory,
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
