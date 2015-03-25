/*
 * Copyright 2009 Mustard Grain, Inc., 2009-2010 LinkedIn, Inc.
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

package voldemort.common.nio;

import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * SelectorManagerWorker manages a Selector, SocketChannel, and IO streams
 * implementation. At the point that the run method is invoked, the Selector
 * with which the (socket) Channel has been registered has notified us that the
 * socket has data to read or write.
 * <p/>
 * The bulk of the complexity in this class surrounds partial reads and writes,
 * as well as determining when all the data needed for the request has been
 * read.
 */

public abstract class SelectorManagerWorker implements Runnable {

    protected final Selector selector;

    protected final SocketChannel socketChannel;

    protected final int socketBufferSize;

    protected final int resizeThreshold;

    protected ByteBufferBackedInputStream inputStream;

    protected ByteBufferBackedOutputStream outputStream;

    protected final long createTimestamp;

    protected final AtomicBoolean isClosed;

    protected final Logger logger = Logger.getLogger(getClass());

    public SelectorManagerWorker(Selector selector,
                                 SocketChannel socketChannel,
                                 int socketBufferSize) {
        this.selector = selector;
        this.socketChannel = socketChannel;
        this.socketBufferSize = socketBufferSize;
        this.resizeThreshold = socketBufferSize * 2; // This is arbitrary...

        this.createTimestamp = System.nanoTime();
        this.isClosed = new AtomicBoolean(false);

        if(logger.isDebugEnabled())
            logger.debug("Accepting remote connection from " + socketChannel.socket());
    }

    protected abstract void read(SelectionKey selectionKey) throws IOException;

    protected abstract void write(SelectionKey selectionKey) throws IOException;

    protected abstract void initializeStreams(int socketBufferSize,
                                              CommBufferSizeStats commBufferStats);

    /**
     * Returns the nanosecond-based timestamp of when this was created.
     * 
     * @return Nanosecond-based timestamp of creation
     */

    public long getCreateTimestamp() {
        return createTimestamp;
    }

    public void run() {
        try {
            SelectionKey selectionKey = socketChannel.keyFor(selector);

            if(selectionKey.isReadable())
                read(selectionKey);
            else if(selectionKey.isWritable())
                write(selectionKey);
            else if(!selectionKey.isValid())
                throw new IllegalStateException("Selection key not valid for "
                                                + socketChannel.socket());
            else
                throw new IllegalStateException("Unknown state, not readable, writable, or valid for "
                                                + socketChannel.socket());
        } catch(ClosedByInterruptException e) {
            close();
        } catch(CancelledKeyException e) {
            close();
        } catch(EOFException e) {
            close();
        } catch(IOException e) {
            logger.info("Connection reset from " + socketChannel.socket() + " with message - "
                        + e.getMessage());
            close();
        } catch(Throwable t) {
            if(logger.isEnabledFor(Level.ERROR))
                logger.error(t.getMessage(), t);

            close();
        }
    }

    public void close() {
        // Due to certain code paths, close may be called in a recursive
        // fashion. Rather than trying to handle all of the cases, simply keep
        // track of whether we've been called before and only perform the logic
        // once.
        if(!isClosed.compareAndSet(false, true))
            return;

        closeInternal();
    }

    protected void closeInternal() {
        if(logger.isDebugEnabled())
            logger.debug("Closing remote connection from " + socketChannel.socket());

        try {
            socketChannel.socket().close();
        } catch(IOException e) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(e.getMessage(), e);
        }

        try {
            socketChannel.close();
        } catch(IOException e) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(e.getMessage(), e);
        }

        SelectionKey selectionKey = socketChannel.keyFor(selector);

        if(selectionKey != null) {
            try {
                selectionKey.attach(null);
                selectionKey.cancel();
            } catch(Exception e) {
                if(logger.isEnabledFor(Level.WARN))
                    logger.warn(e.getMessage(), e);
            }
        }

        // close the streams, so we account for comm buffer frees
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);
    }

    public boolean isClosed() {
        return isClosed.get();
    }

    protected void handleIncompleteRequest(int newPosition) {
        if(logger.isTraceEnabled())
            traceInputBufferState("Incomplete read request detected, before update");

        inputStream.getBuffer().position(newPosition);
        inputStream.getBuffer().limit(inputStream.getBuffer().capacity());

        if(logger.isTraceEnabled())
            traceInputBufferState("Incomplete read request detected, after update");

        if(!inputStream.getBuffer().hasRemaining()) {
            // We haven't read all the data needed for the request AND we
            // don't have enough data in our buffer. So expand it. Note:
            // doubling the current buffer size is arbitrary.
            inputStream.growBuffer();

            if(logger.isTraceEnabled())
                traceInputBufferState("Expanded input buffer");
        }
    }

    protected void traceInputBufferState(String preamble) {
        logger.trace(preamble + " - position: " + inputStream.getBuffer().position() + ", limit: "
                     + inputStream.getBuffer().limit() + ", remaining: "
                     + inputStream.getBuffer().remaining() + ", capacity: "
                     + inputStream.getBuffer().capacity() + " - for " + socketChannel.socket());
    }

}
