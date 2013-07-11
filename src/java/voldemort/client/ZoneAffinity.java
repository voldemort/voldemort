/*
 * Copyright 2012 LinkedIn, Inc
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

/**
 * Encapsulates the timeouts, in ms, for various Voldemort operations
 * 
 */
public class ZoneAffinity {

    private boolean getOpZoneAffinity;
    private boolean getAllOpZoneAffinity;
    private boolean putOpZoneAffinity;

    public ZoneAffinity() {
        this(false, false, false);
    }

    public ZoneAffinity(boolean globalZoneAffinity) {
        this(globalZoneAffinity, globalZoneAffinity, globalZoneAffinity);
    }

    public ZoneAffinity(boolean getOpZoneAffinity,
                              boolean getAllOpZoneAffinity,
                              boolean putOpZoneAffinity) {
        this.getOpZoneAffinity = getOpZoneAffinity;
        this.getAllOpZoneAffinity = getAllOpZoneAffinity;
        this.putOpZoneAffinity = putOpZoneAffinity;
    }

    public boolean getGetOpZoneAffinity() {
        return getOpZoneAffinity;
    }

    public boolean getGetAllOpZoneAffinity() {
        return getAllOpZoneAffinity;
    }

    public boolean getPutOpZoneAffinity() {
        return putOpZoneAffinity;
    }
}
