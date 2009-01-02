package voldemort.server.http.gui;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import voldemort.client.HttpStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.SerializerFactory;

import com.google.common.collect.Maps;

/**
 * Add client for queries, do data parsing
 * 
 * @author jay
 * 
 */
public class QueryServlet extends HttpServlet {

    private static final long serialVersionUID = 1;

    private VelocityEngine engine;
    private StoreClientFactory clientFactory;
    private SerializerFactory serializerFactory;
    private URI uri;

    public QueryServlet() {}

    public QueryServlet(VelocityEngine engine, URI bootstrap) {
        this.engine = engine;
        this.clientFactory = new HttpStoreClientFactory(1, uri.toString());
        this.serializerFactory = new DefaultSerializerFactory();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
            IOException {
        super.doGet(req, resp);
        Map m = Maps.newHashMap();
        engine.render("query.vm", m, resp.getOutputStream());
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        super.doPost(req, resp);
        String storeName = req.getParameter("store");
        String key = req.getParameter("key");
        String keyType = req.getParameter("key_type");
        String value = req.getParameter("value");
        String action = req.getParameter("action");
        Object keyObj = parseKey(keyType, key);

        Map<String, Object> params = new HashMap<String, Object>();
        StoreClient<?, ?> client = clientFactory.getStoreClient(storeName);
        engine.render("query.vm", params, resp.getOutputStream());
    }

    @Override
    public void init() throws ServletException {
        super.init();
        // this.getServletContext().getAttribute("velocity");
    }

    private Object parseKey(String keyType, String key) {
        if ("int8".equals(keyType))
            return Byte.valueOf(key);
        else if ("int16".equals(keyType))
            return Short.valueOf(key);
        else if ("int32".equals(keyType))
            return Integer.valueOf(key);
        else if ("int64".equals(keyType))
            return Long.valueOf(key);
        else if ("string".equals(keyType))
            return key;
        else
            throw new IllegalArgumentException("Unsupported key type: " + keyType);

    }
}
