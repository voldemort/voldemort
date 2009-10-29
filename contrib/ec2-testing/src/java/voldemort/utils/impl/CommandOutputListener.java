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
 * One of the main ways we determine what's going on is to literally parse the
 * output from the remote command line invocation. Each remote operation is
 * different and needs to be implemented specifically for the target command.
 * 
 * <p/>
 * 
 * <b>Note</b>: this approach is very brittle as it relies upon a consistent
 * output from the command. If the client or server are deployed with logging
 * disabled, there is then no way for the local system to detect state,
 * progress, etc. So this mechanism should not be used for anything much more
 * than logging or incidental state management.
 * 
 * @author Kirk True
 */

public interface CommandOutputListener {

    /**
     * Called by the UnixCommand as it receives a line of output and calls any
     * listener that was provided.
     * 
     * @param hostName External host name from which the line of output
     *        originated
     * @param line Line of output from remote system
     */

    public void outputReceived(String hostName, String line);

}
