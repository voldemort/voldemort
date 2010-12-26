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

package voldemort.server.http.gui;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import voldemort.client.ClientConfig;
import voldemort.client.HttpStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;

import com.google.common.collect.Maps;

/**
 * Add client for queries, do data parsing
 * 
 * Note that this is still work-in-progress, see:
 * 
 * http://code.google.com/p/project-voldemort/issues/detail?id=61
 * 
 * 
 */
@SuppressWarnings("all")
public class QueryServlet extends HttpServlet {

    private static final long serialVersionUID = 1;

    private VelocityEngine engine;
    private StoreClientFactory clientFactory;

    public QueryServlet() {}

    public QueryServlet(VelocityEngine engine, URI bootstrap) {
        this.engine = engine;
        this.clientFactory = new HttpStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrap.toString())
                                                                          .setMaxThreads(1));
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
            IOException {
        super.doGet(req, resp);
        Map<String, Object> m = Maps.newHashMap();
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
        if("int8".equals(keyType))
            return Byte.valueOf(key);
        else if("int16".equals(keyType))
            return Short.valueOf(key);
        else if("int32".equals(keyType))
            return Integer.valueOf(key);
        else if("int64".equals(keyType))
            return Long.valueOf(key);
        else if("string".equals(keyType))
            return key;
        else
            throw new IllegalArgumentException("Unsupported key type: " + keyType);

    }
}
