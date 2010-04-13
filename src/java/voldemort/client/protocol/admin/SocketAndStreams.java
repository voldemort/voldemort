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

package voldemort.client.protocol.admin;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

import voldemort.client.protocol.RequestFormatType;

/**
 * A wrapper class that wraps a socket with its DataInputStream and
 * DataOutputStream
 * 
 * 
 */
public class SocketAndStreams {

    private static final int DEFAULT_BUFFER_SIZE = 1000;

    private final Socket socket;
    private final RequestFormatType requestFormatType;
    private final DataInputStream inputStream;
    private final DataOutputStream outputStream;
    private final long createTimestamp;

    public SocketAndStreams(Socket socket, RequestFormatType requestFormatType) throws IOException {
        this(socket, DEFAULT_BUFFER_SIZE, requestFormatType);
    }

    public SocketAndStreams(Socket socket, int bufferSizeBytes, RequestFormatType type)
                                                                                       throws IOException {
        this.socket = socket;
        this.inputStream = new DataInputStream(new BufferedInputStream(socket.getInputStream(),
                                                                       bufferSizeBytes));
        this.outputStream = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(),
                                                                          bufferSizeBytes));
        this.requestFormatType = type;
        this.createTimestamp = System.nanoTime();
    }

    public Socket getSocket() {
        return socket;
    }

    public DataInputStream getInputStream() {
        return inputStream;
    }

    public DataOutputStream getOutputStream() {
        return outputStream;
    }

    public RequestFormatType getRequestFormatType() {
        return this.requestFormatType;
    }

    /**
     * Returns the nanosecond-based timestamp of when this socket was created.
     * 
     * @return Nanosecond-based timestamp of socket creation
     * 
     * @see SocketResourceFactory#validate(SocketDestination, SocketAndStreams)
     */

    public long getCreateTimestamp() {
        return createTimestamp;
    }

}
