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

package voldemort.store.socket;

import voldemort.utils.Utils;

import com.google.common.base.Objects;

/**
 * A host + port
 * 
 * @author jay
 * 
 */
public class SocketDestination {

    private final String host;
    private final int port;

    public SocketDestination(String host, int port) {
        this.host = Utils.notNull(host);
        this.port = Utils.notNull(port);
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == this)
            return true;

        if(obj == null || !obj.getClass().equals(SocketDestination.class))
            return false;

        SocketDestination d = (SocketDestination) obj;
        return getHost().equals(d.getHost()) && getPort() == d.getPort();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(host, port);
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }

}
