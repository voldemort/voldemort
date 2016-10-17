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

package voldemort.client;

/**
 * Encapsulates the zone affinity configuration for various Voldemort operations
 */
public class ZoneAffinity {

    private boolean getOpZoneAffinity;
    private boolean getAllOpZoneAffinity;
    private boolean getVersionsOpZoneAffinity;

    public ZoneAffinity() {
        this(false, false, false);
    }

    public ZoneAffinity(boolean globalZoneAffinity) {
        this(globalZoneAffinity, globalZoneAffinity, globalZoneAffinity);
    }

    public ZoneAffinity(boolean getOpZoneAffinity,
                        boolean getAllOpZoneAffinity,
                        boolean getVersionsOpZoneAffinity) {
        this.getOpZoneAffinity = getOpZoneAffinity;
        this.getAllOpZoneAffinity = getAllOpZoneAffinity;
        this.getVersionsOpZoneAffinity = getVersionsOpZoneAffinity;
    }

    public ZoneAffinity(ZoneAffinity source) {
        this(source.getOpZoneAffinity,
             source.getAllOpZoneAffinity,
             source.getVersionsOpZoneAffinity);
    }

    public boolean isGetOpZoneAffinityEnabled() {
        return getOpZoneAffinity;
    }

    public boolean isGetAllOpZoneAffinityEnabled() {
        return getAllOpZoneAffinity;
    }

    public boolean isGetVersionsOpZoneAffinityEnabled() {
        return getVersionsOpZoneAffinity;
    }

    /**
     * @param enabled Defines if zone affinity is applied for GET operation
     */
    public ZoneAffinity setEnableGetOpZoneAffinity(boolean enabled) {
        getOpZoneAffinity = enabled;
        return this;
    }

    /**
     * @param enabled Defines if zone affinity is applied for GETALL operation
     */
    public ZoneAffinity setEnableGetAllOpZoneAffinity(boolean enabled) {
        getAllOpZoneAffinity = enabled;
        return this;
    }

    /**
     * @param enabled Defines if zone affinity is applied for PUT operation
     */
    public ZoneAffinity setEnableGetVersionsOpZoneAffinity(boolean enabled) {
        getVersionsOpZoneAffinity = enabled;
        return this;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " [getOpZoneAffinity=" + getOpZoneAffinity
               + ", getAllOpZoneAffinity=" + getAllOpZoneAffinity + ", getVersionsOpZoneAffinity="
               + getVersionsOpZoneAffinity + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj) {
            return true;
        }
        if(obj == null) {
            return false;
        }
        if(getClass() != obj.getClass()) {
            return false;
        }
        ZoneAffinity other = (ZoneAffinity) obj;
        if(getAllOpZoneAffinity != other.getAllOpZoneAffinity) {
            return false;
        }
        if(getOpZoneAffinity != other.getOpZoneAffinity) {
            return false;
        }
        if(getVersionsOpZoneAffinity != other.getVersionsOpZoneAffinity) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (getAllOpZoneAffinity ? 1231 : 1237);
        result = prime * result + (getOpZoneAffinity ? 1231 : 1237);
        result = prime * result + (getVersionsOpZoneAffinity ? 1231 : 1237);
        return result;
    }
}
