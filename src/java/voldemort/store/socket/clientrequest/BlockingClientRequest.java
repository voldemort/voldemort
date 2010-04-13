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

public class BlockingClientRequest<T> implements ClientRequest<T> {

    private final ClientRequest<T> delegate;

    private final CountDownLatch latch;

    public BlockingClientRequest(ClientRequest<T> delegate) {
        this.delegate = delegate;
        latch = new CountDownLatch(1);
    }

    public void await() throws InterruptedException {
        latch.await();
    }

    public void completed() {
        delegate.completed();
        latch.countDown();
    }

    public void setServerError(Exception e) {
        delegate.setServerError(e);
    }

    public T getResult() {
        return delegate.getResult();
    }

    public boolean isCompleteResponse(ByteBuffer buffer) {
        return delegate.isCompleteResponse(buffer);
    }

    public void parseResponse(DataInputStream inputStream) throws IOException {
        delegate.parseResponse(inputStream);
    }

    public void formatRequest(DataOutputStream outputStream) throws IOException {
        delegate.formatRequest(outputStream);
    }

}
