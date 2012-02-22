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

import voldemort.VoldemortException;
import voldemort.store.StoreTimeoutException;
import voldemort.store.UnreachableStoreException;

/**
 * AbstractClientRequest implements ClientRequest to provide some basic
 * mechanisms that most implementations will need.
 * 
 * @param <T> Return type
 */

public abstract class AbstractClientRequest<T> implements ClientRequest<T> {

    private T result;

    private Exception error;

    private volatile boolean isComplete = false;

    private volatile boolean isParsed = false;

    private volatile boolean isTimedOut = false;

    protected abstract void formatRequestInternal(DataOutputStream outputStream) throws IOException;

    protected abstract T parseResponseInternal(DataInputStream inputStream) throws IOException;

    public boolean formatRequest(DataOutputStream outputStream) {
        try {
            formatRequestInternal(outputStream);
        } catch(IOException e) {
            error = e;
            return false;
        } catch(VoldemortException e) {
            error = e;
            return false;
        }

        return true;
    }

    public void parseResponse(DataInputStream inputStream) {
        try {
            result = parseResponseInternal(inputStream);
        } catch(IOException e) {
            error = e;
        } catch(VoldemortException e) {
            error = e;
        } finally {
            isParsed = true;
        }
    }

    public T getResult() throws VoldemortException, IOException {
        if(isTimedOut)
            throw new StoreTimeoutException("Request timed out");

        if(!isComplete)
            throw new IllegalStateException("Client response not complete, cannot determine result");

        if(!isParsed)
            throw new UnreachableStoreException("Client response not read/parsed, cannot determine result");

        if(error instanceof IOException)
            throw (IOException) error;
        else if(error instanceof VoldemortException)
            throw (VoldemortException) error;

        return result;
    }

    public final void complete() {
        isComplete = true;
    }

    public boolean isComplete() {
        return isComplete;
    }

    public final void timeOut() {
        isTimedOut = true;
    }

    public boolean isTimedOut() {
        return isTimedOut;
    }

}
