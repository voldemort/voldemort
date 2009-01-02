package voldemort.store.http;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PutMethod;

import voldemort.VoldemortException;
import voldemort.store.Store;
import voldemort.store.StoreUtils;
import voldemort.store.UnreachableStoreException;
import voldemort.utils.ByteUtils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A remote store client that transmits operations via HTTP and interacts with
 * the VoldemortHttpServer.
 * 
 * @author jay
 */
public class HttpStore implements Store<byte[], byte[]> {

    private static final Hex codec = new Hex();
    private static final HttpResponseCodeErrorMapper httpResponseCodeErrorMapper = new HttpResponseCodeErrorMapper();
    private static final String VERSION_EXTENSION = "X-vldmt-version";

    private final String storeName;
    private final String host;
    private final int port;
    private final HttpClient httpClient;

    public HttpStore(String storeName, String host, int port, HttpClient client) {
        this.storeName = storeName;
        this.host = host;
        this.port = port;
        this.httpClient = client;
    }

    public boolean delete(byte[] key, Version version) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        String url = getUrl(key);
        DeleteMethod method = null;
        try {
            method = new DeleteMethod(url);
            VectorClock clock = (VectorClock) version;
            method.setRequestHeader(VERSION_EXTENSION,
                                    new String(Base64.encodeBase64(clock.toBytes())));
            int response = httpClient.executeMethod(method);
            if (response == HttpURLConnection.HTTP_NOT_FOUND)
                return false;
            if (response != HttpURLConnection.HTTP_OK)
                httpResponseCodeErrorMapper.throwError(response, method.getStatusText());
            return true;
        } catch (HttpException e) {
            throw new VoldemortException(e);
        } catch (IOException e) {
            throw new UnreachableStoreException("Could not connect to " + url + " for " + storeName,
                                                e);
        } finally {
            if (method != null)
                method.releaseConnection();
        }
    }

    public List<Versioned<byte[]>> get(byte[] key) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        String url = getUrl(key);
        GetMethod method = null;
        try {
            method = new GetMethod(url);
            int response = httpClient.executeMethod(method);
            if (response != HttpURLConnection.HTTP_OK)
                httpResponseCodeErrorMapper.throwError(response, method.getStatusText());
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(method.getResponseBody()));
            List<Versioned<byte[]>> items = new ArrayList<Versioned<byte[]>>();
            try {
                while (true) {
                    int size = input.readInt();
                    byte[] bytes = new byte[size];
                    input.read(bytes);
                    VectorClock clock = new VectorClock(bytes);
                    byte[] data = ByteUtils.copy(bytes, clock.sizeInBytes(), bytes.length);
                    items.add(new Versioned<byte[]>(data, clock));
                }
            } catch (EOFException e) {
                input.close();
            }
            return items;
        } catch (HttpException e) {
            throw new VoldemortException(e);
        } catch (IOException e) {
            throw new UnreachableStoreException("Could not connect to " + url + " for " + storeName,
                                                e);
        } finally {
            if (method != null)
                method.releaseConnection();
        }
    }

    public void put(byte[] key, Versioned<byte[]> versioned) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        String url = getUrl(key);
        PutMethod method = null;
        try {
            method = new PutMethod(url);
            VectorClock clock = (VectorClock) versioned.getVersion();
            method.setRequestHeader(VERSION_EXTENSION,
                                    new String(Base64.encodeBase64(clock.toBytes()), "UTF-8"));
            method.setRequestEntity(new ByteArrayRequestEntity(versioned.getValue()));
            int response = httpClient.executeMethod(method);
            if (response != HttpURLConnection.HTTP_OK)
                httpResponseCodeErrorMapper.throwError(response, method.getStatusText());
        } catch (HttpException e) {
            throw new VoldemortException(e);
        } catch (IOException e) {
            throw new UnreachableStoreException("Could not connect to " + url + " for " + storeName,
                                                e);
        } finally {
            if (method != null)
                method.releaseConnection();
        }
    }

    public void close() {}

    public String getName() {
        return storeName;
    }

    private String getUrl(byte[] key) throws VoldemortException {
        return "http://" + host + ":" + port + "/" + getName() + "/"
               + ByteUtils.getString(codec.encode(key), "UTF-8");
    }

}
