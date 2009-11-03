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
import static voldemort.utils.impl.CommandLineParameterizer.VOLDEMORT_HOME_DIRECTORY_PARAM;
import static voldemort.utils.impl.CommandLineParameterizer.VOLDEMORT_NODE_ID_PARAM;
import static voldemort.utils.impl.CommandLineParameterizer.VOLDEMORT_ROOT_DIRECTORY_PARAM;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import voldemort.utils.ClusterStarter;
import voldemort.utils.RemoteOperationException;

/**
 * SshClusterStarter is an implementation of ClusterStarter that essentially
 * just SSH's into all of the machines and runs the voldemort-server.sh script.
 * We set the node ID specific to each server from the command line vs. storing
 * it in the server.properties file.
 * 
 * @author Kirk True
 */

public class SshClusterStarter extends CommandLineRemoteOperation implements ClusterStarter {

    private final AtomicInteger completedCounter = new AtomicInteger();

    private final CommandOutputListener outputListener = new SshClusterStarterCommandOutputListener();

    private final Collection<String> hostNames;

    private final File sshPrivateKey;

    private final String hostUserId;

    private final String voldemortRootDirectory;

    private final String voldemortHomeDirectory;

    private final Map<String, Integer> nodeIds;

    /**
     * Creates a new SshClusterStarter instance.
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
     * @param voldemortHomeDirectory Directory under which Voldemort
     *        configuration and data are kept, relative to the home directory of
     *        the user on the remote system represented by hostUserId; this is
     *        the same value as represented by the $VOLDEMORT_HOME environment
     *        variable; assumed to be the same for all of the remote hosts
     * @param nodeIds Node number for each of the remote hosts; this is the
     *        value that can appear in server.properties but we specify on
     *        launch of the instance
     */

    public SshClusterStarter(Collection<String> hostNames,
                             File sshPrivateKey,
                             String hostUserId,
                             String voldemortRootDirectory,
                             String voldemortHomeDirectory,
                             Map<String, Integer> nodeIds) {
        this.hostNames = hostNames;
        this.sshPrivateKey = sshPrivateKey;
        this.hostUserId = hostUserId;
        this.voldemortRootDirectory = voldemortRootDirectory;
        this.voldemortHomeDirectory = voldemortHomeDirectory;
        this.nodeIds = nodeIds;
    }

    public void execute() throws RemoteOperationException {
        if(logger.isInfoEnabled())
            logger.info("Starting Voldemort cluster");

        CommandLineParameterizer commandLineParameterizer = new CommandLineParameterizer("SshClusterStarter.ssh");
        Map<String, String> hostNameCommandLineMap = new HashMap<String, String>();

        for(String hostName: hostNames) {
            Map<String, String> parameters = new HashMap<String, String>();
            parameters.put(HOST_NAME_PARAM, hostName);
            parameters.put(HOST_USER_ID_PARAM, hostUserId);
            parameters.put(SSH_PRIVATE_KEY_PARAM, sshPrivateKey.getAbsolutePath());
            parameters.put(VOLDEMORT_ROOT_DIRECTORY_PARAM, voldemortRootDirectory);
            parameters.put(VOLDEMORT_HOME_DIRECTORY_PARAM, voldemortHomeDirectory);
            parameters.put(VOLDEMORT_NODE_ID_PARAM, nodeIds.get(hostName).toString());

            hostNameCommandLineMap.put(hostName, commandLineParameterizer.parameterize(parameters));
        }

        // We don't actually exit until the server applications quit, so we
        // don't have the 'cluster startup complete' logging here - it's done in
        // SshClusterStarterCommandOutputListener.
        execute(hostNameCommandLineMap);
    }

    @Override
    protected Callable<Object> getCallable(UnixCommand command) {
        CommandOutputListener commandOutputListener = new LoggingCommandOutputListener(outputListener,
                                                                                       logger,
                                                                                       true);
        return new ClusterStarterCallable<Object>(command, commandOutputListener);
    }

    public class SshClusterStarterCommandOutputListener implements CommandOutputListener {

        public void outputReceived(String hostName, String line) {
            if(line.contains("Startup completed")) {
                completedCounter.incrementAndGet();

                if(logger.isInfoEnabled()) {
                    logger.info(hostName + " startup complete");

                    if(hasStartupCompleted())
                        logger.info("Cluster startup complete");
                }
            }
        }
    }

    private boolean hasStartupCompleted() {
        return hostNames.size() == completedCounter.get();
    }

    private class ClusterStarterCallable<T> implements Callable<T> {

        private final UnixCommand command;

        private final CommandOutputListener commandOutputListener;

        public ClusterStarterCallable(UnixCommand command,
                                      CommandOutputListener commandOutputListener) {
            this.command = command;
            this.commandOutputListener = commandOutputListener;
        }

        public T call() throws Exception {
            int exitCode = command.execute(commandOutputListener);

            // If the user hits Ctrl+C after startup, we get an exit code of
            // 255, so don't throw an exception in this case.
            if(!(exitCode == 255 && hasStartupCompleted()))
                throw new Exception("Process on " + command.getHostName() + " exited with code "
                                    + exitCode + ". Please check the logs for details.");

            return null;
        }
    }

}
