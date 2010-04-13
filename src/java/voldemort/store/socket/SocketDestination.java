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

import voldemort.client.protocol.RequestFormatType;
import voldemort.utils.Utils;

import com.google.common.base.Objects;

/**
 * A host + port + protocol
 * 
 * 
 */
public class SocketDestination {

    private final String host;
    private final int port;
    private final RequestFormatType requestFormatType;
    private long lastClosedTimestamp;

    public SocketDestination(String host, int port, RequestFormatType requestFormatType) {
        this.host = Utils.notNull(host);
        this.port = Utils.notNull(port);
        this.requestFormatType = Utils.notNull(requestFormatType);
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public RequestFormatType getRequestFormatType() {
        return requestFormatType;
    }

    /**
     * Returns the nanosecond-based timestamp of when this socket destination
     * was last closed. SocketDestination objects can be closed when their node
     * is marked as unavailable if the node goes down (temporarily or
     * otherwise). This timestamp is used to determine when sockets related to
     * the SocketDestination should be closed.
     * 
     * <p/>
     * 
     * This value starts off as 0 and is updated via setLastClosedTimestamp each
     * time the node is marked as unavailable.
     * 
     * @return Nanosecond-based timestamp of last close
     * 
     * @see SocketDestination#setLastClosedTimestamp()
     * @see ClientRequestExecutorResourceFactory#validate(SocketDestination, SocketAndStreams)
     */

    public long getLastClosedTimestamp() {
        return lastClosedTimestamp;
    }

    /**
     * Assigns the last closed timestamp based on the current time in
     * nanoseconds.
     * 
     * <p/>
     * 
     * This value starts off as 0 and is updated via this method each time the
     * node is marked as unavailable.
     * 
     * @see SocketDestination#getLastClosedTimestamp()
     * @see ClientRequestExecutorResourceFactory#validate(SocketDestination, SocketAndStreams)
     */

    public void setLastClosedTimestamp() {
        this.lastClosedTimestamp = System.nanoTime();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == this)
            return true;

        if(obj == null || !obj.getClass().equals(SocketDestination.class))
            return false;

        SocketDestination d = (SocketDestination) obj;
        return getHost().equals(d.getHost()) && getPort() == d.getPort()
               && getRequestFormatType().equals(d.getRequestFormatType());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(host, port, requestFormatType);
    }

    @Override
    public String toString() {
        return host + ":" + port + "(" + requestFormatType.getCode() + ")";
    }

}
