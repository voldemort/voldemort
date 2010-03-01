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

/**
 * StdOutCommandOutputListener simply takes the output from the remote system
 * and logs it using System.out/System.err.
 * 
 */

public class StdOutCommandOutputListener extends DelegatingCommandOutputListener {

    private final boolean shouldProcessExceptions;

    /**
     * Creates a new instance of LoggingCommandOutputListener.
     * 
     * @param delegate Delegate to call after logging, or null if not used
     * @param shouldProcessExceptions True to <i>attempt</i> to process
     *        exceptions and write them as warnings rather than info; not
     *        fool-proof by any means
     */

    public StdOutCommandOutputListener(CommandOutputListener delegate,
                                       boolean shouldProcessExceptions) {
        super(delegate);
        this.shouldProcessExceptions = shouldProcessExceptions;
    }

    @Override
    public void outputReceived(String hostName, String line) {
        // If desired we can increase the checking of the exception to make it
        // more reliable differentiate real problems.
        if(shouldProcessExceptions && (line.contains("Exception") || line.startsWith("\tat"))) {
            System.err.println(hostName + ": " + line);
        } else {
            System.out.println(hostName + ": " + line);
        }

        super.outputReceived(hostName, line);
    }

}
