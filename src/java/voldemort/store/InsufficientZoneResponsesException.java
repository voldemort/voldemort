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

import java.util.Collection;
import java.util.Collections;

import voldemort.VoldemortException;

/**
 * Thrown if an operation fails due to too few reachable nodes.
 * 
 * 
 */
public class InsufficientZoneResponsesException extends VoldemortException {

    private static final long serialVersionUID = 1L;

    private Collection<? extends Throwable> causes;

    public InsufficientZoneResponsesException(String s, Throwable e) {
        super(s, e);
        causes = Collections.singleton(e);
    }

    public InsufficientZoneResponsesException(String s) {
        super(s);
        causes = Collections.emptyList();
    }

    public InsufficientZoneResponsesException(Throwable e) {
        super(e);
        causes = Collections.singleton(e);
    }

    public InsufficientZoneResponsesException(Collection<? extends Throwable> failures) {
        this("Insufficient number of zones.", failures);
    }

    public InsufficientZoneResponsesException(String message,
                                              Collection<? extends Throwable> failures) {
        super(message, failures.size() > 0 ? failures.iterator().next() : null);
        this.causes = failures;
    }

    public Collection<? extends Throwable> getCauses() {
        return this.causes;
    }

}
