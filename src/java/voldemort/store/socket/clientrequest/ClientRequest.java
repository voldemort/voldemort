/*
 * Copyright 2010 LinkedIn, Inc
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

package voldemort.store.socket.clientrequest;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormat;

/**
 * ClientRequest represents a <b>single</b> request/response combination to a
 * remote Voldemort instance -- a new instance is created for each request to
 * the server.
 * 
 * <p/>
 * 
 * This class is used to support both blocking and non-blocking communication
 * with a remote server.
 * 
 * @param <T> Type of data that is returned by the request
 */

public interface ClientRequest<T> {

    /**
     * Once completed has been called, this will return the result of the
     * request <b>or</b> thrown an error if the request wasn't completed.
     * 
     * @return Result or an exception is thrown if the request failed
     */

    public T getResult() throws VoldemortException, IOException;

    /**
     * This eventually calls into a nested {@link RequestFormat} instance's
     * writeXxx method. The ClientRequest actually buffers all I/O, so the data
     * written via formatRequest is actually inserted into a {@link ByteBuffer}
     * which is later sent over the wire to the server.
     * 
     * <p/>
     * 
     * This is used internally by the {@link ClientRequest} logic and should not
     * be invoked by users of the sub-system.
     * 
     * @param outputStream Write the request to this output stream
     */

    public boolean formatRequest(DataOutputStream outputStream);

    /**
     * isCompleteResponse determines if the response that the
     * {@link ClientRequestExecutor}'s received thus far is inclusive of the
     * entire response. This relies on the {@link RequestFormat} instance's
     * isCompleteXxxResponse methods.
     * 
     * <p/>
     * 
     * This is used internally by the {@link ClientRequest} logic and should not
     * be invoked by users of the sub-system.
     * 
     * @param buffer ByteBuffer containing the data received thus far
     * 
     * @return True if the buffer contains the complete response, false if it
     *         only includes part of the response.
     */

    public boolean isCompleteResponse(ByteBuffer buffer);

    /**
     * Parses the response from the server to turn it into a result. If this
     * causes an application-level error to arise, it should not be thrown here,
     * but instead stored until {@link #getResult()} is called.
     * 
     * <p/>
     * 
     * This is used internally by the {@link ClientRequest} logic and should not
     * be invoked by users of the sub-system.
     * 
     * @param inputStream InputStream from which to read the response
     */

    public void parseResponse(DataInputStream inputStream);

    /**
     * Called by the {@link ClientRequestExecutor} once all the processing
     * (normal or abnormal) has occurred on the {@link ClientRequest} object.
     * This exists mainly to implement blocking operations whereby we need to
     * have a mechanism to unblock the caller waiting for the response.
     * 
     * <p/>
     * 
     * This is used internally by the {@link ClientRequest} logic and should not
     * be invoked by users of the sub-system.
     */

    public void complete();

    /**
     * Returns <code>true</code> if {@link #complete()} was called.
     * 
     * @return <code>true</code> if completed, <code>false</code> otherwise
     */

    public boolean isComplete();

    /**
     * Called by the {@link ClientRequestExecutor} after it has timed out. This
     * is different from the complete call, since the timeout event needs to be
     * notified to the caller in a special way.
     * <p/>
     * 
     * This is used internally by the {@link ClientRequest} logic and should not
     * be invoked by users of the sub-system.
     */

    public void timeOut();

    /**
     * Returns <code>true</code> if {@link ClientRequestExecutor} timed out.
     * 
     * @return <code>true</code> if timed out, <code>false</code> otherwise
     */

    public boolean isTimedOut();
}