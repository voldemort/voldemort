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
 * Indicates that the given store cannot be reached (say, due to network
 * failure).
 * 
 * 
 */
public class UnreachableStoreException extends VoldemortException {

    private static final long serialVersionUID = 1L;

    public UnreachableStoreException(String s) {
        super(s);
    }

    public UnreachableStoreException(String s, Throwable t) {
        super(s, t);
    }

    public static UnreachableStoreException wrap(String s, Throwable t) {
        if(t instanceof UnreachableStoreException) {
            return new UnreachableStoreException(s, t.getCause());
        }
        return new UnreachableStoreException(s, t);
    }

}
