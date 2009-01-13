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

package voldemort;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class BlockingOutputStream extends OutputStream {

    private BlockingQueue<Byte> queue;

    public BlockingOutputStream() {
        this(new ArrayBlockingQueue<Byte>(1000));
    }

    public BlockingOutputStream(BlockingQueue<Byte> queue) {
        super();
        this.queue = queue;
    }

    @Override
    public void write(int b) throws IOException {
        try {
            queue.put((byte) b);
        } catch(InterruptedException e) {
            throw new IOException(e.getMessage());
        }
    }

}
