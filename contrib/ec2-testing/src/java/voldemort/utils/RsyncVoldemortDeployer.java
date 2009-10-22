package voldemort.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.StringUtils;

public class RsyncVoldemortDeployer implements VoldemortDeployer {

    public void deploy(Collection<String> hostNames,
                       String hostUserId,
                       File sshPrivateKey,
                       File sourceDirectory,
                       String clusterXml_,
                       String storesXml_,
                       String serverProperties_,
                       String destinationDirectory) throws VoldemortDeploymentException {
        for(String hostName: hostNames) {
            List<String> command = new ArrayList<String>();
            command.add("rsync");
            command.add("-vaz");
            command.add("--delete");
            command.add("--progress");
            command.add("--exclude=.git");
            command.add("-e");
            command.add("ssh -o StrictHostKeyChecking=no -i " + sshPrivateKey);
            command.add(sourceDirectory.getAbsolutePath());
            command.add(hostUserId + "@" + hostName + ":" + destinationDirectory);

            System.out.println(StringUtils.join(command, " "));

            UnixCommand unixCommand = new UnixCommand(command);

            try {
                unixCommand.execute();
            } catch(Exception e) {
                throw new VoldemortDeploymentException(e);
            }
        }
    }

}