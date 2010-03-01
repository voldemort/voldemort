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

package voldemort.store;

import voldemort.VoldemortException;

/**
 * This exception indicates that the server was unable to initialize on or more
 * storage services or stores within a service.
 * 
 * 
 */
public class StorageInitializationException extends VoldemortException {

    private final static long serialVersionUID = 1;

    public StorageInitializationException() {}

    public StorageInitializationException(String message) {
        super(message);
    }

    public StorageInitializationException(Throwable t) {
        super(t);
    }

    public StorageInitializationException(String message, Throwable t) {
        super(message, t);
    }

}
