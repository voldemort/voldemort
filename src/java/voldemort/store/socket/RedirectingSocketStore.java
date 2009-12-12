package voldemort.store.socket;

import java.util.List;

import voldemort.VoldemortException;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

public class RedirectingSocketStore extends SocketStore {

    public RedirectingSocketStore(String name,
                                  SocketDestination dest,
                                  SocketPool socketPool,
                                  boolean reroute) {
        super(name, dest, socketPool, reroute);
    }

    public List<Versioned<byte[]>> getIgnoreInvalidMetadata(ByteArray key)
            throws VoldemortException {
        return super.get(key, true);
    }
}
