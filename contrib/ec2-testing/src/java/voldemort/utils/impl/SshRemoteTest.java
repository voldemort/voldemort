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
import static voldemort.utils.impl.CommandLineParameterizer.REMOTE_TEST_ARGUMENTS_PARAM;
import static voldemort.utils.impl.CommandLineParameterizer.SSH_PRIVATE_KEY_PARAM;
import static voldemort.utils.impl.CommandLineParameterizer.VOLDEMORT_HOME_DIRECTORY_PARAM;
import static voldemort.utils.impl.CommandLineParameterizer.VOLDEMORT_ROOT_DIRECTORY_PARAM;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import voldemort.utils.RemoteOperationException;
import voldemort.utils.RemoteTest;
import voldemort.utils.RemoteTestResult;

public class SshRemoteTest extends CommandLineRemoteOperation<RemoteTestResult> implements
        RemoteTest {

    private final Collection<String> hostNames;

    private final File sshPrivateKey;

    private final String hostUserId;

    private final String voldemortRootDirectory;

    private final String voldemortHomeDirectory;

    private final Map<String, String> remoteTestArguments;

    public SshRemoteTest(Collection<String> hostNames,
                         File sshPrivateKey,
                         String hostUserId,
                         String voldemortRootDirectory,
                         String voldemortHomeDirectory,
                         Map<String, String> remoteTestArguments) {
        this.hostNames = hostNames;
        this.sshPrivateKey = sshPrivateKey;
        this.hostUserId = hostUserId;
        this.voldemortRootDirectory = voldemortRootDirectory;
        this.voldemortHomeDirectory = voldemortHomeDirectory;
        this.remoteTestArguments = remoteTestArguments;
    }

    public List<RemoteTestResult> execute() throws RemoteOperationException {
        if(logger.isInfoEnabled())
            logger.info("Executing remote tests");

        CommandLineParameterizer commandLineParameterizer = new CommandLineParameterizer("SshRemoteTest.ssh");
        Map<String, String> hostNameCommandLineMap = new HashMap<String, String>();

        for(String hostName: hostNames) {
            Map<String, String> parameters = new HashMap<String, String>();
            parameters.put(HOST_NAME_PARAM, hostName);
            parameters.put(HOST_USER_ID_PARAM, hostUserId);
            parameters.put(SSH_PRIVATE_KEY_PARAM, sshPrivateKey.getAbsolutePath());
            parameters.put(VOLDEMORT_ROOT_DIRECTORY_PARAM, voldemortRootDirectory);
            parameters.put(VOLDEMORT_HOME_DIRECTORY_PARAM, voldemortHomeDirectory);
            parameters.put(REMOTE_TEST_ARGUMENTS_PARAM, remoteTestArguments.get(hostName));

            hostNameCommandLineMap.put(hostName, commandLineParameterizer.parameterize(parameters));
        }

        return execute(hostNameCommandLineMap);
    }

    @Override
    protected Callable<RemoteTestResult> getCallable(UnixCommand command) {
        RemoteTestResult remoteTestResult = new RemoteTestResult(command.getHostName());
        RemoteTestOutputParser remoteTestOutputParser = new RemoteTestOutputParser(logger,
                                                                                   remoteTestResult);
        CommandOutputListener commandOutputListener = new LoggingCommandOutputListener(remoteTestOutputParser,
                                                                                       logger);
        return new RemoteTestResultCallable(command, commandOutputListener, remoteTestResult);
    }

    protected class RemoteTestResultCallable implements Callable<RemoteTestResult> {

        private final UnixCommand command;

        private final CommandOutputListener commandOutputListener;

        private final RemoteTestResult remoteTestResult;

        public RemoteTestResultCallable(UnixCommand command,
                                        CommandOutputListener commandOutputListener,
                                        RemoteTestResult remoteTestResult) {
            this.command = command;
            this.commandOutputListener = commandOutputListener;
            this.remoteTestResult = remoteTestResult;
        }

        public RemoteTestResult call() throws Exception {
            command.execute(commandOutputListener);

            return remoteTestResult;
        }

    }

}
