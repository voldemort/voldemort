package voldemort.store.socket.clientrequest;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.common.nio.ByteBufferBackedOutputStream;
import voldemort.store.StoreTimeoutException;
import voldemort.store.UnreachableStoreException;
import voldemort.store.nonblockingstore.NonblockingStoreCallback;
import voldemort.store.socket.SocketDestination;
import voldemort.store.stats.ClientSocketStats;
import voldemort.utils.Time;
import voldemort.utils.Utils;
import voldemort.utils.pool.KeyedResourcePool;

public class NonblockingStoreCallbackClientRequest<T> implements ClientRequest<T> {

    private final SocketDestination destination;
    private final ClientRequest<T> clientRequest;
    private final ClientRequestExecutor clientRequestExecutor;
    private final NonblockingStoreCallback callback;
    private final long startNs;
    private final KeyedResourcePool<SocketDestination, ClientRequestExecutor> executorPool;
    private final ClientSocketStats stats;
    private final Logger logger = Logger.getLogger(getClass());

    private volatile boolean isComplete;

    public NonblockingStoreCallbackClientRequest(KeyedResourcePool<SocketDestination, ClientRequestExecutor> executorPool,
                                                 SocketDestination destination,
                                                 ClientRequest<T> clientRequest,
                                                 ClientRequestExecutor clientRequestExecutor,
                                                 NonblockingStoreCallback callback,
                                                 ClientSocketStats stats) {
        this.executorPool = executorPool;
        this.destination = destination;
        this.clientRequest = clientRequest;
        this.clientRequestExecutor = clientRequestExecutor;
        this.callback = callback;
        this.startNs = System.nanoTime();
        this.stats = stats;
    }

    private void invokeCallback(Object o, long requestTime) {
        if(callback != null) {
            try {
                if(logger.isDebugEnabled()) {
                    logger.debug("Async request end; requestRef: "
                                 + System.identityHashCode(clientRequest)
                                 + " time: "
                                 + System.currentTimeMillis()
                                 + " server: "
                                 + clientRequestExecutor.getSocketChannel()
                                                        .socket()
                                                        .getRemoteSocketAddress()
                                 + " local socket: "
                                 + clientRequestExecutor.getSocketChannel()
                                                        .socket()
                                                        .getLocalAddress() + ":"
                                 + clientRequestExecutor.getSocketChannel().socket().getLocalPort()
                                 + " result: " + o);
                }
                callback.requestComplete(o, requestTime);
            } catch(Exception e) {
                if(logger.isEnabledFor(Level.WARN))
                    logger.warn(e, e);
            }
        }
    }

    @Override
    public void complete() {
        try {
            clientRequest.complete();
            Object result = clientRequest.getResult();
            long durationNs = Utils.elapsedTimeNs(startNs, System.nanoTime());
            if(stats != null) {
                stats.recordAsyncOpTimeNs(destination, durationNs);
            }
            invokeCallback(result, durationNs / Time.NS_PER_MS);
        } catch(Exception e) {
            invokeCallback(e, (System.nanoTime() - startNs) / Time.NS_PER_MS);
        } finally {
            isComplete = true;
            // checkin may throw a (new) exception. Any prior exception
            // has been passed off via invokeCallback.
            executorPool.checkin(destination, clientRequestExecutor);
        }
    }

    public void reportException(IOException e) {
        clientRequest.reportException(e);
    }

    @Override
    public boolean isComplete() {
        return isComplete;
    }

    @Override
    public boolean formatRequest(ByteBufferBackedOutputStream outputStream) {
        return clientRequest.formatRequest(outputStream);
    }

    @Override
    public T getResult() throws VoldemortException, UnreachableStoreException {
        return clientRequest.getResult();
    }

    @Override
    public boolean isCompleteResponse(ByteBuffer buffer) {
        return clientRequest.isCompleteResponse(buffer);
    }

    @Override
    public void parseResponse(DataInputStream inputStream) {
        clientRequest.parseResponse(inputStream);
    }

    @Override
    public void timeOut() {
        clientRequest.timeOut();
        invokeCallback(new StoreTimeoutException("ClientRequestExecutor timed out. Cannot complete request."),
                       (System.nanoTime() - startNs) / Time.NS_PER_MS);
        executorPool.checkin(destination, clientRequestExecutor);
    }

    @Override
    public boolean isTimedOut() {
        return clientRequest.isTimedOut();
    }
}