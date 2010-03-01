/*
 * Copyright 2009 LinkedIn, Inc.
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

package voldemort.utils;

/**
 * HostNamePair represents a pairing of a host's external and internal names.
 * Depending on the network topology, a given server may be referenced by a
 * different name to systems outside its local network than the name used
 * internal to the local network.
 * 
 * <p/>
 * 
 * An EC2 instance, as an example, has two different host names. The external
 * name (e.g. ec2-72-44-40-78.compute-1.amazonaws.com) and an internal one
 * (domU-12-31-39-06-BE-25.compute-1.internal).
 * 
 * <p/>
 * 
 * For systems which have only one name, both the external and internal host
 * names should be the same. That is, they should be identical and neither
 * should be set to null.
 * 
 */

public class HostNamePair {

    private final String externalHostName;

    private final String internalHostName;

    public HostNamePair(String externalHostName, String internalHostName) {
        if(externalHostName == null)
            throw new IllegalArgumentException("externalHostName must be non-null");

        if(internalHostName == null)
            throw new IllegalArgumentException("externalHostName must be non-null");

        this.externalHostName = externalHostName;
        this.internalHostName = internalHostName;
    }

    public String getExternalHostName() {
        return externalHostName;
    }

    public String getInternalHostName() {
        return internalHostName;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + externalHostName.hashCode();
        result = prime * result + internalHostName.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if(this == other)
            return true;

        if(!(other instanceof HostNamePair))
            return false;

        HostNamePair hnp = (HostNamePair) other;

        return hnp.externalHostName.equals(externalHostName)
               && hnp.internalHostName.equals(internalHostName);
    }

}
