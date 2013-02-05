/*
 * Copyright 2013 LinkedIn, Inc
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

package voldemort.client.protocol.admin;

import voldemort.VoldemortException;
import voldemort.versioning.ObsoleteVersionException;

public class RepairEntryResult {

    private final VoldemortException voldemortException;
    private final boolean success;

    RepairEntryResult() {
        this.voldemortException = null;
        success = true;
    }

    RepairEntryResult(VoldemortException e) {
        if(e instanceof ObsoleteVersionException) {
            this.voldemortException = null;
            this.success = true;
        } else {
            this.voldemortException = e;
            this.success = false;
        }
    }

    public boolean isSuccess() {
        return success;
    }

    public boolean hasException() {
        return (voldemortException != null);
    }

    public VoldemortException getException() {
        return voldemortException;
    }

}