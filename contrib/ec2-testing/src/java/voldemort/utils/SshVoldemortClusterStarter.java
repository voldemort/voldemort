package voldemort.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.StringUtils;

public class SshVoldemortClusterStarter implements VoldemortClusterStarter {

    public void start(Collection<String> hostNames,
                      String hostUserId,
                      File sshPrivateKey,
                      String voldemortRootDirectory,
                      String voldemortHomeDirectory) throws VoldemortStartClusterException {
        for(String hostName: hostNames) {
            List<String> command = new ArrayList<String>();
            command.add("ssh");
            command.add("-i");
            command.add(sshPrivateKey.getAbsolutePath());
            command.add(hostUserId + "@" + hostName);
            command.add("export VOLDEMORT_HOME=$(pwd)/" + voldemortHomeDirectory + " ; cd "
                        + voldemortRootDirectory
                        + " ; nohup ./bin/voldemort-server.sh > ~/log.txt 2>&1 &");

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
