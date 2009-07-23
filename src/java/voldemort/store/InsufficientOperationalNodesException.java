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

/**
 * Thrown if an operation fails due to too few reachable nodes.
 * 
 * @author jay
 * 
 */
public class InsufficientOperationalNodesException extends StoreOperationFailureException {

    private static final long serialVersionUID = 1L;

    private Collection<? extends Throwable> causes;

    public InsufficientOperationalNodesException(String s, Throwable e) {
        super(s, e);
        causes = Collections.singleton(e);
    }

    public InsufficientOperationalNodesException(String s) {
        super(s);
        causes = Collections.emptyList();
    }

    public InsufficientOperationalNodesException(Throwable e) {
        super(e);
        causes = Collections.singleton(e);
    }

    public InsufficientOperationalNodesException(Collection<? extends Throwable> failures) {
        this("Insufficient operational nodes to immediately satisfy request.", failures);
    }

    public InsufficientOperationalNodesException(String message,
                                                 Collection<? extends Throwable> failures) {
        super(message, failures.size() > 0 ? failures.iterator().next() : null);
        this.causes = failures;
    }

    public Collection<? extends Throwable> getCauses() {
        return this.causes;
    }

    @Override
    public short getId() {
        return 2;
    }
}
