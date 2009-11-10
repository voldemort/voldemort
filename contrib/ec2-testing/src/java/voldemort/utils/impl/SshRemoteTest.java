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
import static voldemort.utils.impl.CommandLineParameterizer.TEST_COMMAND_PARAM;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import voldemort.utils.RemoteOperationException;
import voldemort.utils.RemoteTest;

/**
 * SshRemoteTest is an SSH-based implementation of RemoteTest that wraps calls
 * to voldemort.performance.RemoteTest (AKA voldemort-remote-test.sh) on all of
 * the machines provided.
 * 
 * @author Kirk True
 */

public class SshRemoteTest extends CommandLineRemoteOperation implements RemoteTest {

    private final Collection<String> hostNames;

    private final File sshPrivateKey;

    private final String hostUserId;

    private final Map<String, String> commands;

    /**
     * Creates a new SshRemoteTest instance.
     * 
     * @param hostNames External host names for servers that make up the
     *        Voldemort cluster
     * @param sshPrivateKey SSH private key file on local filesystem that can
     *        access all of the remote hosts, or null if not needed
     * @param hostUserId User ID on the remote hosts; assumed to be the same for
     *        all of the remote hosts
     * @param commands Per-test host commands to run, specific to each host
     * 
     * @see voldemort.performance.RemoteTest
     */

    public SshRemoteTest(Collection<String> hostNames,
                         File sshPrivateKey,
                         String hostUserId,
                         Map<String, String> commands) {
        this.hostNames = hostNames;
        this.sshPrivateKey = sshPrivateKey;
        this.hostUserId = hostUserId;
        this.commands = commands;
    }

    public void execute() throws RemoteOperationException {
        if(logger.isInfoEnabled())
            logger.info("Executing remote tests");

        CommandLineParameterizer commandLineParameterizer = new CommandLineParameterizer("SshRemoteTest.ssh"
                                                                                         + (sshPrivateKey != null ? ""
                                                                                                                 : ".nokey"));
        Map<String, String> hostNameCommandLineMap = new HashMap<String, String>();

        int index = 0;

        for(String hostName: hostNames) {
            Map<String, String> parameters = new HashMap<String, String>();
            parameters.put(HOST_NAME_PARAM, hostName);
            parameters.put(HOST_USER_ID_PARAM, hostUserId);
            parameters.put(SSH_PRIVATE_KEY_PARAM,
                           sshPrivateKey != null ? sshPrivateKey.getAbsolutePath() : null);
            parameters.put(TEST_COMMAND_PARAM, commands.get(hostName));

            hostNameCommandLineMap.put(hostName, commandLineParameterizer.parameterize(parameters));

            index++;
        }

        execute(hostNameCommandLineMap);

        if(logger.isInfoEnabled())
            logger.info("Execution of remote tests complete");
    }

    @Override
    protected Callable<?> getCallable(UnixCommand command) {
        CommandOutputListener commandOutputListener = new StdOutCommandOutputListener(null, true);
        return new ExitCodeCallable(command, commandOutputListener);
    }

}
