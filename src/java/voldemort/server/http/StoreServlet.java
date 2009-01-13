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

package voldemort.server.http;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;

import voldemort.VoldemortException;
import voldemort.server.VoldemortServer;
import voldemort.store.Store;
import voldemort.store.http.HttpResponseCodeErrorMapper;
import voldemort.utils.ByteUtils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.common.base.Join;
import com.google.common.base.Objects;

/**
 * Handles requests from HttpStores and multiplexes them to the appropriate
 * sub-store for actual storage
 * 
 * @author jay
 * 
 */
public class StoreServlet extends HttpServlet {

    private static final Pattern SLASH_PATTERN = Pattern.compile("/");
    private static final long serialVersionUID = 1;
    private static final String VERSION_EXTENSION = "X-vldmt-version";
    private static final HttpResponseCodeErrorMapper httpResponseCodeErrorMapper = new HttpResponseCodeErrorMapper();
    private static final Hex urlCodec = new Hex();

    private ConcurrentMap<String, Store<byte[], byte[]>> stores;

    /* For use by servlet container */
    public StoreServlet() {}

    public StoreServlet(ConcurrentMap<String, Store<byte[], byte[]>> stores) {
        this.stores = stores;
    }

    @Override
    public void init() throws ServletException {
        super.init();
        // if we don't already have a stores map, attempt to initialize from the
        // servlet context
        if(this.stores == null) {
            ServletContext context = this.getServletContext();
            VoldemortServer server = (VoldemortServer) Objects.nonNull(context.getAttribute(VoldemortServletContextListener.SERVER_CONFIG_KEY));
            this.stores = server.getStoreMap();
        }
    }

    private Store<byte[], byte[]> getStore(String name) {
        Store<byte[], byte[]> store = stores.get(name);
        if(store == null)
            throw new VoldemortException("No store named '" + name + "'.");
        return store;
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        String[] path = SLASH_PATTERN.split(request.getPathInfo());
        byte[] key = getKey(path);
        String storeName = getStoreName(path);
        Store<byte[], byte[]> store = getStore(storeName);
        DataOutputStream stream = new DataOutputStream(response.getOutputStream());
        try {
            List<Versioned<byte[]>> values = store.get(key);
            for(Versioned<byte[]> versioned: values) {
                byte[] clock = ((VectorClock) versioned.getVersion()).toBytes();
                byte[] value = versioned.getValue();
                stream.writeInt(clock.length + value.length);
                stream.write(clock);
                stream.write(value);
            }
        } catch(VoldemortException v) {
            HttpResponseCodeErrorMapper.ResponseCode code = httpResponseCodeErrorMapper.mapErrorToResponseCode(v);
            response.setContentType("text/xml");
            response.sendError(code.getCode(), errorXml(v, code.getText()));
        }
    }

    @Override
    protected void doPut(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        String[] path = SLASH_PATTERN.split(request.getPathInfo());
        byte[] key = getKey(path);
        String storeName = getStoreName(path);
        Store<byte[], byte[]> store = getStore(storeName);
        int size = request.getContentLength();
        byte[] contents = new byte[size];
        request.getInputStream().read(contents);
        try {
            VectorClock clock = new VectorClock(Base64.decodeBase64(request.getHeader(VERSION_EXTENSION)
                                                                           .getBytes()));
            store.put(key, new Versioned<byte[]>(contents, clock));
        } catch(VoldemortException v) {
            HttpResponseCodeErrorMapper.ResponseCode code = httpResponseCodeErrorMapper.mapErrorToResponseCode(v);
            response.setContentType("text/xml");
            response.sendError(code.getCode(), errorXml(v, code.getText()));
        }
    }

    @Override
    public void doDelete(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        String[] path = SLASH_PATTERN.split(request.getPathInfo());
        byte[] key = getKey(path);
        String storeName = getStoreName(path);
        Store<byte[], byte[]> store = getStore(storeName);
        try {
            byte[] versionBytes = ByteUtils.getBytes(request.getHeader(VERSION_EXTENSION), "UTF-8");
            VectorClock clock = new VectorClock(Base64.decodeBase64(versionBytes));
            boolean succeeded = store.delete(key, clock);
            if(!succeeded)
                response.sendError(HttpURLConnection.HTTP_NOT_FOUND);
        } catch(VoldemortException v) {
            HttpResponseCodeErrorMapper.ResponseCode code = httpResponseCodeErrorMapper.mapErrorToResponseCode(v);
            response.setContentType("text/xml");
            response.sendError(code.getCode(), errorXml(v, code.getText()));
        }
    }

    public static String getKey(String url) {
        String[] path = url.split("/");
        return path[path.length - 1];
    }

    public String errorXml(VoldemortException type, String message) {
        return "<?xml version='1.0' encoding='UTF-8'?>" + "<error>" + "<name>"
               + type.getClass().getName() + "</name>" + "<message>" + message + "</message>"
               + "</error>";
    }

    private byte[] getKey(String[] urlPieces) {
        if(urlPieces.length < 2)
            throw new VoldemortException("Invalid request for " + Join.join(".", urlPieces)
                                         + ": must specify both a store and key.");

        String keyStr = urlPieces[urlPieces.length - 1];
        try {
            byte[] key = ByteUtils.getBytes(keyStr, "UTF-8");
            return urlCodec.decode(key);
        } catch(DecoderException e) {
            throw new VoldemortException("Corrupt key format.");
        }
    }

    private String getStoreName(String[] urlPieces) {
        if(urlPieces.length < 2)
            throw new VoldemortException("Invalid request for " + Join.join("/", urlPieces)
                                         + ": must specify both a store and key.");
        return urlPieces[urlPieces.length - 2];
    }

}
