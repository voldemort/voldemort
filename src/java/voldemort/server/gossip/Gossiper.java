package voldemort.server.gossip;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author afeinberg
 */
public class Gossiper implements Runnable {
    private final AtomicBoolean running = new AtomicBoolean(true);

    public void stop() {
        running.set(false);
    }

    @Override
    public void run() {
        while (running.get()) {
            
        }
    }
}
