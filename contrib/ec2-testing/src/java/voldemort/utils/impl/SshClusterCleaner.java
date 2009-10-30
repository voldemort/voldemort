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

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import voldemort.utils.ClusterCleaner;
import voldemort.utils.RemoteOperationException;

/**
 * SshClusterCleaner is an implementation of ClusterCleaner that cleans a
 * particular data directory.
 * 
 * @author Kirk True
 */

public class SshClusterCleaner extends CommandLineRemoteOperation<Object> implements ClusterCleaner {

    private final Collection<String> hostNames;

    private final File sshPrivateKey;

    private final String hostUserId;

    private final String voldemortHomeDirectory;

    /**
     * Creates a new SshClusterCleaner instance.
     * 
     * @param hostNames External host names for which to clean the Voldemort
     *        data directory
     * @param sshPrivateKey SSH private key file on local filesystem that can
     *        access all of the remote hosts
     * @param hostUserId User ID on the remote hosts; assumed to be the same for
     *        all of the remote hosts
     * @param voldemortHomeDirectory Directory under which Voldemort
     *        configuration and data are kept, relative to the home directory of
     *        the user on the remote system represented by hostUserId; this is
     *        the same value as represented by the $VOLDEMORT_HOME environment
     *        variable; assumed to be the same for all of the remote hosts
     */

    public SshClusterCleaner(Collection<String> hostNames,
                             File sshPrivateKey,
                             String hostUserId,
                             String voldemortHomeDirectory) {
        this.hostNames = hostNames;
        this.sshPrivateKey = sshPrivateKey;
        this.hostUserId = hostUserId;
        this.voldemortHomeDirectory = voldemortHomeDirectory;
    }

    public List<Object> execute() throws RemoteOperationException {
        if(logger.isInfoEnabled())
            logger.info("Cleaning " + voldemortHomeDirectory + " on remote hosts: " + hostNames);

        CommandLineParameterizer commandLineParameterizer = new CommandLineParameterizer("SshClusterCleaner.ssh");
        Map<String, String> hostNameCommandLineMap = new HashMap<String, String>();

        for(String hostName: hostNames) {
            Map<String, String> parameters = new HashMap<String, String>();
            parameters.put(HOST_NAME_PARAM, hostName);
            parameters.put(HOST_USER_ID_PARAM, hostUserId);
            parameters.put(SSH_PRIVATE_KEY_PARAM, sshPrivateKey.getAbsolutePath());
            parameters.put(VOLDEMORT_HOME_DIRECTORY_PARAM, voldemortHomeDirectory);

            hostNameCommandLineMap.put(hostName, commandLineParameterizer.parameterize(parameters));
        }

        List<Object> ret = execute(hostNameCommandLineMap);

        if(logger.isInfoEnabled())
            logger.info("Cleaning remote hosts complete");

        return ret;
    }

}