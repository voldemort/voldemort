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

package voldemort.versioning;

import voldemort.VoldemortApplicationException;

/**
 * An exception that indicates an attempt by the user to overwrite a newer value
 * for a given key with an older value for the same key. This is a
 * application-level error, and indicates the application has attempted to write
 * stale data.
 * 
 * 
 */
public class ObsoleteVersionException extends VoldemortApplicationException {

    private static final long serialVersionUID = 1L;

    public ObsoleteVersionException(String message) {
        super(message);
    }

    public ObsoleteVersionException(String message, Exception cause) {
        super(message, cause);
    }

    /**
     * Override to avoid the overhead of stack trace. For a given store there is
     * really only one method (put) that can throw this so retaining the stack
     * trace is not useful
     */
    @Override
    public Throwable fillInStackTrace() {
        return this;
    }

}
