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

import java.util.concurrent.Callable;

import voldemort.utils.RemoteTest;
import voldemort.utils.RemoteTestResult;

public class SshRemoteTest extends CommandLineRemoteOperation<RemoteTestResult> implements
        RemoteTest {

    public SshRemoteTest(RemoteOperationConfig commandLineClusterConfig) {
        super(commandLineClusterConfig, "SshRemoteTest.ssh");
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
