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
 * Abstract implementation of CommandOutputListener that allows chaining of
 * calls to outputReceived.
 * 
 * @author Kirk True
 */

public abstract class DelegatingCommandOutputListener implements CommandOutputListener {

    private final CommandOutputListener delegate;

    /**
     * Constructs a new DelegatingCommandOutputListener instance. Note: some
     * implementations may only <i>optionally</i> use delegation; that is, it's
     * not required. In those cases, simply pass in <code>null</code>.
     * 
     * @param delegate CommandOutputListener to delegate calls to, or null if
     *        unused
     */

    protected DelegatingCommandOutputListener(CommandOutputListener delegate) {
        this.delegate = delegate;
    }

    public void outputReceived(String hostName, String line) {
        if(delegate != null)
            delegate.outputReceived(hostName, line);
    }

}
