package voldemort.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.StringUtils;

public class SshVoldemortClusterStopper implements VoldemortClusterStopper {

    public void stop(Collection<String> hostNames,
                     String hostUserId,
                     File sshPrivateKey,
                     String voldemortRootDirectory) throws VoldemortStartClusterException {
        for(String hostName: hostNames) {
            List<String> command = new ArrayList<String>();
            command.add("ssh");
            command.add("-i");
            command.add(sshPrivateKey.getAbsolutePath());
            command.add(hostUserId + "@" + hostName);
            command.add("cd " + voldemortRootDirectory + " ; ./bin/voldemort-stop.sh");

            System.out.println(StringUtils.join(command, " "));

            UnixCommand unixCommand = new UnixCommand(command);

            try {
                unixCommand.execute();
            } catch(Exception e) {
                throw new VoldemortStartClusterException(e);
            }
        }
    }

}
