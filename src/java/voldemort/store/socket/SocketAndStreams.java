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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

/**
 * A wrapper class that wraps a socket with its DataInputStream and
 * DataOutputStream
 * 
 * @author jay
 * 
 */
public class SocketAndStreams {

    private static final int DEFAULT_BUFFER_SIZE = 1000;

    private final Socket socket;
    private final DataInputStream inputStream;
    private final DataOutputStream outputStream;

    public SocketAndStreams(Socket socket) throws IOException {
        this(socket, DEFAULT_BUFFER_SIZE);
    }

    public SocketAndStreams(Socket socket, int bufferSizeBytes) throws IOException {
        this.socket = socket;
        this.inputStream = new DataInputStream(new BufferedInputStream(socket.getInputStream(),
                                                                       bufferSizeBytes));
        this.outputStream = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(),
                                                                          bufferSizeBytes));
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

}
