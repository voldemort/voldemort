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
 * This exception indicates that a store is disabled, which tells the server
 * it should go in offline mode [1].
 *
 * A Read-Only store can become disabled when a Build and Push job failed for
 * a small subset of servers (typically, replication factor - 1), in which
 * case the other servers (which successfully fetched the data) would be
 * allowed to swap and serve the new data, while the server(s) which failed
 * would see that store be disabled, in order to avoid serving stale data.
 *
 * [1] Currently, putting the server in offline mode is the only way we have
 * to stop serving stale data and have the clients reliably mark the node as
 * down and try other replicas. Eventually, we could implement fancier
 * solutions to achieve the same while also keeping the healthy stores up and
 * running.
 */
public class DisabledStoreException extends VoldemortException {

    private final static long serialVersionUID = 1;

    public DisabledStoreException() {}

    public DisabledStoreException(String message) {
        super(message);
    }

    public DisabledStoreException(Throwable t) {
        super(t);
    }

    public DisabledStoreException(String message, Throwable t) {
        super(message, t);
    }

}
