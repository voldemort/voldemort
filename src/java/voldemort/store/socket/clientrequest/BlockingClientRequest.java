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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import voldemort.VoldemortException;

/**
 * BlockingClientRequest is used to implement blocking IO using the non-blocking
 * IO-based {@link ClientRequest} logic. Essentially it wraps a vanilla
 * ClientRequest and adds an {@link #await() method} on which the caller will
 * wait for completion (either success or failure).
 * 
 * @param <T> Type of data that is returned by the request
 */

public class BlockingClientRequest<T> implements ClientRequest<T> {

    private final ClientRequest<T> delegate;

    private final CountDownLatch latch;

    private final long timeoutMs;

    public BlockingClientRequest(ClientRequest<T> delegate, long timeoutMs) {
        this.delegate = delegate;
        this.timeoutMs = timeoutMs;
        latch = new CountDownLatch(1);
    }

    public void complete() {
        delegate.complete();
        latch.countDown();
    }

    public boolean isComplete() {
        return delegate.isComplete() && latch.getCount() == 0;
    }

    public void await() throws InterruptedException {
        latch.await(timeoutMs, TimeUnit.MILLISECONDS);
    }

    public T getResult() throws VoldemortException, IOException {
        return delegate.getResult();
    }

    public boolean isCompleteResponse(ByteBuffer buffer) {
        return delegate.isCompleteResponse(buffer);
    }

    public void parseResponse(DataInputStream inputStream) {
        delegate.parseResponse(inputStream);
    }

    public boolean formatRequest(DataOutputStream outputStream) {
        return delegate.formatRequest(outputStream);
    }

    public void timeOut() {
        delegate.timeOut();
    }

    public boolean isTimedOut() {
        return delegate.isTimedOut();
    }

}
