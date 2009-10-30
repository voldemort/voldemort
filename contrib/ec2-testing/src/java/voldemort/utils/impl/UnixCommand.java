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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

/**
 * A wrapper for executing a UNIX command.
 * 
 * @author Jay Kreps
 */

public class UnixCommand {

    private final String hostName;

    private final String[] args;

    public UnixCommand(String hostName, String... args) {
        this.hostName = hostName;
        this.args = args;
    }

    public UnixCommand(String hostName, List<String> args) {
        this.hostName = hostName;
        this.args = args.toArray(new String[args.size()]);
    }

    public String getHostName() {
        return hostName;
    }

    public int execute(CommandOutputListener commandOutputListener) throws InterruptedException,
            IOException {
        ProcessBuilder builder = new ProcessBuilder(this.args);
        Process process = builder.start();

        Thread stdoutWatcher = new ProcessOutputWatcher(new BufferedReader(new InputStreamReader(process.getInputStream())),
                                                        commandOutputListener);
        Thread stderrWatcher = new ProcessOutputWatcher(new BufferedReader(new InputStreamReader(process.getErrorStream())),
                                                        commandOutputListener);

        stdoutWatcher.start();
        stderrWatcher.start();

        process.waitFor();

        return process.exitValue();
    }

    private class ProcessOutputWatcher extends Thread {

        private final BufferedReader reader;

        private final CommandOutputListener commandOutputListener;

        public ProcessOutputWatcher(BufferedReader reader,
                                    CommandOutputListener commandOutputListener) {
            this.reader = reader;
            this.commandOutputListener = commandOutputListener;
        }

        @Override
        public void run() {
            try {
                String line = null;

                while((line = reader.readLine()) != null) {
                    try {
                        commandOutputListener.outputReceived(hostName, line);
                    } catch(Throwable t) {
                        // This is just in case any CommandOutputReceiver has
                        // bugs or something we don't die on all output.
                        t.printStackTrace();
                    }
                }
            } catch(IOException e) {
                e.printStackTrace();
            }
        }
    }

}
