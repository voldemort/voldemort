package voldemort.store.gzip;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;

import voldemort.VoldemortException;
import voldemort.store.DelegatingStore;
import voldemort.store.Store;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

/**
 * A Store decorator that gzips and ungzips its contents as it stores them
 * 
 * @author jay
 * 
 */
public class GzipStore<K> extends DelegatingStore<K, byte[]> implements Store<K, byte[]> {

    public GzipStore(Store<K, byte[]> innerStore) {
        super(innerStore);
    }

    public List<Versioned<byte[]>> get(K key) throws VoldemortException {
        List<Versioned<byte[]>> found = getInnerStore().get(key);
        List<Versioned<byte[]>> results = new ArrayList<Versioned<byte[]>>(found.size());
        try {
            for (Versioned<byte[]> item : found)
                results.add(new Versioned<byte[]>(IOUtils.toByteArray(new GZIPInputStream(new ByteArrayInputStream(item.getValue()))),
                                                  (VectorClock) item.getVersion()));
        } catch (IOException e) {
            throw new VoldemortException(e);
        }

        return results;
    }

    public void put(K key, Versioned<byte[]> value) throws VoldemortException {
        try {
            getInnerStore().put(key,
                                new Versioned<byte[]>(IOUtils.toByteArray(new GZIPInputStream(new ByteArrayInputStream(value.getValue()))),
                                                      (VectorClock) value.getVersion()));
        } catch (IOException e) {
            throw new VoldemortException(e);
        }
    }

}
