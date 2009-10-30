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

import static voldemort.utils.impl.CommandLineParameterizer.BOOTSTRAP_URL_PARAM;
import static voldemort.utils.impl.CommandLineParameterizer.HOST_NAME_PARAM;
import static voldemort.utils.impl.CommandLineParameterizer.HOST_USER_ID_PARAM;
import static voldemort.utils.impl.CommandLineParameterizer.ITERATIONS_PARAM;
import static voldemort.utils.impl.CommandLineParameterizer.NUM_REQUESTS_PARAM;
import static voldemort.utils.impl.CommandLineParameterizer.OPERATIONS_PARAM;
import static voldemort.utils.impl.CommandLineParameterizer.RAMP_TIME_PARAM;
import static voldemort.utils.impl.CommandLineParameterizer.SSH_PRIVATE_KEY_PARAM;
import static voldemort.utils.impl.CommandLineParameterizer.START_KEY_INDEX_PARAM;
import static voldemort.utils.impl.CommandLineParameterizer.STORE_NAME_PARAM;
import static voldemort.utils.impl.CommandLineParameterizer.THREADS_PARAM;
import static voldemort.utils.impl.CommandLineParameterizer.VALUE_SIZE_PARAM;
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

/**
 * SshRemoteTest is an SSH-based implementation of RemoteTest that wraps calls
 * to voldemort.performance.RemoteTest (AKA voldemort-remote-test.sh) on all of
 * the machines provided.
 * 
 * @author Kirk True
 */

public class SshRemoteTest extends CommandLineRemoteOperation<RemoteTestResult> implements
        RemoteTest {

    private final Collection<String> hostNames;

    private final File sshPrivateKey;

    private final String hostUserId;

    private final String voldemortRootDirectory;

    private final String voldemortHomeDirectory;

    private final int rampTime;

    private final String operations;

    private final int valueSize;

    private final int threads;

    private final int iterations;

    private final String bootstrapUrl;

    private final String storeName;

    private final long numRequests;

    /**
     * Creates a new SshRemoteTest instance.
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
     * @param rampTime Value in seconds to wait on each node before starting the
     *        test; the ramp up is multiplied by the number of clients
     *        (n*rampTime); that is, client 0 will wait 0 seconds (0*rampTime),
     *        client 1 will wait rampTime seconds (1*rampTime), client 2 will
     *        wait 2*rampTime seconds (2*rampTime), and so on
     * @param operations A concatenation of the "-r", "-w", and "-d " values of
     *        the voldemort.performance.RemoteTest class
     * @param valueSize See the --value-size argument of the
     *        voldemort.performance.RemoteTest class
     * @param threads See the --threads argument of the
     *        voldemort.performance.RemoteTest class
     * @param iterations See the --iterations argument of the
     *        voldemort.performance.RemoteTest class
     * @param bootstrapUrl See the bootstrapUrl argument of the
     *        voldemort.performance.RemoteTest class
     * @param storeName See the storeName argument of the
     *        voldemort.performance.RemoteTest class
     * @param numRequests See the num-requests argument of the
     *        voldemort.performance.RemoteTest class
     * 
     * @see voldemort.performance.RemoteTest
     */

    public SshRemoteTest(Collection<String> hostNames,
                         File sshPrivateKey,
                         String hostUserId,
                         String voldemortRootDirectory,
                         String voldemortHomeDirectory,
                         int rampTime,
                         String operations,
                         int valueSize,
                         int threads,
                         int iterations,
                         String bootstrapUrl,
                         String storeName,
                         long numRequests) {
        this.hostNames = hostNames;
        this.sshPrivateKey = sshPrivateKey;
        this.hostUserId = hostUserId;
        this.voldemortRootDirectory = voldemortRootDirectory;
        this.voldemortHomeDirectory = voldemortHomeDirectory;
        this.rampTime = rampTime;
        this.operations = operations;
        this.valueSize = valueSize;
        this.threads = threads;
        this.iterations = iterations;
        this.bootstrapUrl = bootstrapUrl;
        this.storeName = storeName;
        this.numRequests = numRequests;
    }

    public List<RemoteTestResult> execute() throws RemoteOperationException {
        if(logger.isInfoEnabled())
            logger.info("Executing remote tests");

        CommandLineParameterizer commandLineParameterizer = new CommandLineParameterizer("SshRemoteTest.ssh");
        Map<String, String> hostNameCommandLineMap = new HashMap<String, String>();

        int index = 0;

        for(String hostName: hostNames) {
            Map<String, String> parameters = new HashMap<String, String>();
            parameters.put(HOST_NAME_PARAM, hostName);
            parameters.put(HOST_USER_ID_PARAM, hostUserId);
            parameters.put(SSH_PRIVATE_KEY_PARAM, sshPrivateKey.getAbsolutePath());
            parameters.put(VOLDEMORT_ROOT_DIRECTORY_PARAM, voldemortRootDirectory);
            parameters.put(VOLDEMORT_HOME_DIRECTORY_PARAM, voldemortHomeDirectory);
            parameters.put(RAMP_TIME_PARAM, String.valueOf(index * rampTime));
            parameters.put(OPERATIONS_PARAM, operations);
            parameters.put(START_KEY_INDEX_PARAM, String.valueOf(index * numRequests));
            parameters.put(VALUE_SIZE_PARAM, String.valueOf(valueSize));
            parameters.put(THREADS_PARAM, String.valueOf(threads));
            parameters.put(ITERATIONS_PARAM, String.valueOf(iterations));
            parameters.put(BOOTSTRAP_URL_PARAM, bootstrapUrl);
            parameters.put(STORE_NAME_PARAM, storeName);
            parameters.put(NUM_REQUESTS_PARAM, String.valueOf(numRequests));

            hostNameCommandLineMap.put(hostName, commandLineParameterizer.parameterize(parameters));

            index++;
        }

        List<RemoteTestResult> ret = execute(hostNameCommandLineMap);

        if(logger.isInfoEnabled())
            logger.info("Execution of remote tests complete");

        return ret;
    }

    @Override
    protected Callable<RemoteTestResult> getCallable(UnixCommand command) {
        RemoteTestResult remoteTestResult = new RemoteTestResult(command.getHostName());
        RemoteTestOutputParser remoteTestOutputParser = new RemoteTestOutputParser(logger,
                                                                                   remoteTestResult);
        CommandOutputListener commandOutputListener = new LoggingCommandOutputListener(remoteTestOutputParser,
                                                                                       logger,
                                                                                       true);
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
