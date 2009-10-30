/*
 * Copyright 2009 LinkedIn, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.utils.impl;

import static voldemort.utils.impl.CommandLineParameterizer.HOST_NAME_PARAM;
import static voldemort.utils.impl.CommandLineParameterizer.HOST_USER_ID_PARAM;
import static voldemort.utils.impl.CommandLineParameterizer.SSH_PRIVATE_KEY_PARAM;
import static voldemort.utils.impl.CommandLineParameterizer.VOLDEMORT_ROOT_DIRECTORY_PARAM;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import voldemort.utils.ClusterStopper;
import voldemort.utils.RemoteOperationException;

/**
 * SshClusterStopper is an implementation of ClusterStopper that essentially
 * just SSH's into all of the machines and runs voldemort-stop.sh.
 * 
 * @author Kirk True
 */

public class SshClusterStopper extends CommandLineRemoteOperation<Object> implements ClusterStopper {

    private final AtomicInteger completedCounter = new AtomicInteger();

    private final CommandOutputListener outputListener = new SshClusterStopperCommandOutputListener();

    private final Collection<String> hostNames;

    private final File sshPrivateKey;

    private final String hostUserId;

    private final String voldemortRootDirectory;

    /**
     * Creates a new SshClusterStopper instance.
     * 
     * @param hostNames External host names for servers that make up the
     *        Voldemort cluster
     * @param sshPrivateKey SSH private key file on local filesystem that can
     *        access all of the remote hosts
     * @param hostUserId User ID on the remote hosts; assumed to be the same for
     *        all of the remote hosts
     * @param voldemortRootDirectory Directory pointing to the Voldemort
     *        distribution, relative to the home directory of the user on the
     *        remote system represented by hostUserId; assumed to be the same
     *        for all of the remote hosts
     */

    public SshClusterStopper(Collection<String> hostNames,
                             File sshPrivateKey,
                             String hostUserId,
                             String voldemortRootDirectory) {
        super();
        this.hostNames = hostNames;
        this.sshPrivateKey = sshPrivateKey;
        this.hostUserId = hostUserId;
        this.voldemortRootDirectory = voldemortRootDirectory;
    }

    public List<Object> execute() throws RemoteOperationException {
        if(logger.isInfoEnabled())
            logger.info("Stopping Voldemort cluster");

        CommandLineParameterizer commandLineParameterizer = new CommandLineParameterizer("SshClusterStopper.ssh");
        Map<String, String> hostNameCommandLineMap = new HashMap<String, String>();

        for(String hostName: hostNames) {
            Map<String, String> parameters = new HashMap<String, String>();
            parameters.put(HOST_NAME_PARAM, hostName);
            parameters.put(HOST_USER_ID_PARAM, hostUserId);
            parameters.put(SSH_PRIVATE_KEY_PARAM, sshPrivateKey.getAbsolutePath());
            parameters.put(VOLDEMORT_ROOT_DIRECTORY_PARAM, voldemortRootDirectory);

            hostNameCommandLineMap.put(hostName, commandLineParameterizer.parameterize(parameters));
        }

        List<Object> ret = execute(hostNameCommandLineMap);

        if(logger.isInfoEnabled())
            logger.info("Stopping of Voldemort cluster complete");

        return ret;
    }

    @Override
    protected Callable<Object> getCallable(UnixCommand command) {
        CommandOutputListener commandOutputListener = new LoggingCommandOutputListener(outputListener,
                                                                                       logger,
                                                                                       true);
        return new ExitCodeCallable<Object>(command, commandOutputListener);
    }

    public class SshClusterStopperCommandOutputListener implements CommandOutputListener {

        public void outputReceived(String hostName, String line) {
            if(line.contains("Stopping Voldemort")) {
                completedCounter.incrementAndGet();

                if(logger.isInfoEnabled()) {
                    logger.info(hostName + " shutdown complete");

                    if(hostNames.size() == completedCounter.get())
                        logger.info("Cluster shutdown complete");
                }
            }
        }
    }

}
