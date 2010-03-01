/*
 * Copyright 2008-2009 LinkedIn, Inc
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

package voldemort.client;

import voldemort.VoldemortException;

/**
 * The exception thrown when bootstrapping the store from the cluster fails
 * (e.g. because none of the given nodes could be connected to). This is
 * generally an unrecoverable failure.
 * 
 * 
 */
public class BootstrapFailureException extends VoldemortException {

    private static final long serialVersionUID = 1L;

    public BootstrapFailureException() {}

    public BootstrapFailureException(String message, Throwable cause) {
        super(message, cause);
    }

    public BootstrapFailureException(String message) {
        super(message);
    }

    public BootstrapFailureException(Throwable cause) {
        super(cause);
    }

}
