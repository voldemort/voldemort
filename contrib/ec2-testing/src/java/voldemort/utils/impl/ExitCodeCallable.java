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

/**
 * ExitCodeCallable is an implementation of Callable to allow the UnixCommand to
 * be executed in the context of a thread pool.
 * 
 * @author Kirk True
 * 
 * @param <T> Type of return object
 */

public class ExitCodeCallable implements Callable<Object> {

    private final UnixCommand command;

    private final CommandOutputListener commandOutputListener;

    public ExitCodeCallable(UnixCommand command, CommandOutputListener commandOutputListener) {
        this.command = command;
        this.commandOutputListener = commandOutputListener;
    }

    public Object call() throws Exception {
        int exitCode = command.execute(commandOutputListener);

        if(exitCode != 0)
            throw new Exception("Process on " + command.getHostName() + " exited with code "
                                + exitCode + ". Please check the logs for details.");

        return null;
    }

}
