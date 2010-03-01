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

package voldemort.serialization;

import voldemort.VoldemortException;

/**
 * Thrown to indicate failures in serialization
 * 
 * This is a user-initiated failure.
 * 
 * 
 */
public class SerializationException extends VoldemortException {

    private static final long serialVersionUID = 1;

    protected SerializationException() {
        super();
    }

    public SerializationException(String s, Throwable t) {
        super(s, t);
    }

    public SerializationException(String s) {
        super(s);
    }

    public SerializationException(Throwable t) {
        super(t);
    }

}
