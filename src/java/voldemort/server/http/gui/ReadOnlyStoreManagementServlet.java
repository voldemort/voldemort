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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.server.ServiceType;
import voldemort.server.VoldemortServer;
import voldemort.server.http.VoldemortServletContextListener;
import voldemort.server.storage.StorageService;
import voldemort.store.StorageEngine;
import voldemort.store.readonly.FileFetcher;
import voldemort.store.readonly.RandomAccessFileStore;
import voldemort.utils.ByteArray;
import voldemort.utils.Utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * A servlet that supports both manual or programatic operations on read only
 * stores. The operations are
 * <ol>
 * <li>FETCH. Fetch the given files to the local node. Parameters:
 * operation=fetch, index=index-file-url, data=data-file-url</li>
 * <li>SWAP. operation=swap, store=store-name, index=index-file-url,
 * data=data-file-url</li>
 * </ol>
 * 
 * @author jay
 * 
 */
public class ReadOnlyStoreManagementServlet extends HttpServlet {

    private static final long serialVersionUID = 1;
    private static final Logger logger = Logger.getLogger(ReadOnlyStoreManagementServlet.class);

    private List<RandomAccessFileStore> stores;
    private VelocityEngine velocityEngine;
    private FileFetcher fileFetcher;

    public ReadOnlyStoreManagementServlet(VoldemortServer server, VelocityEngine engine) {
        this.stores = getReadOnlyStores(server);
        this.velocityEngine = Utils.notNull(engine);
        String className = server.getVoldemortConfig()
                                 .getAllProps()
                                 .getString("file.fetcher.class", null);
        if(className == null) {
            this.fileFetcher = null;
        } else {
            try {
                logger.info("Loading fetcher " + className);
                this.fileFetcher = (FileFetcher) Class.forName(className).newInstance();
            } catch(Exception e) {
                throw new VoldemortException("Error loading file fetcher class " + className);
            }
        }
    }

    @Override
    public void init() throws ServletException {
        super.init();
        this.stores = getReadOnlyStores((VoldemortServer) getServletContext().getAttribute(VoldemortServletContextListener.SERVER_CONFIG_KEY));
        this.velocityEngine = (VelocityEngine) Utils.notNull(getServletContext().getAttribute(VoldemortServletContextListener.VELOCITY_ENGINE_KEY));
    }

    private List<RandomAccessFileStore> getReadOnlyStores(VoldemortServer server) {
        StorageService storage = (StorageService) Utils.notNull(server)
                                                       .getService(ServiceType.STORAGE);
        List<RandomAccessFileStore> l = Lists.newArrayList();
        for(StorageEngine<ByteArray, byte[]> engine: storage.getStoreRepository()
                                                            .getStorageEnginesByClass(RandomAccessFileStore.class)) {
            l.add((RandomAccessFileStore) engine);
        }
        return l;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
            IOException {
        Map<String, Object> params = Maps.newHashMap();
        params.put("stores", stores);
        velocityEngine.render("read-only-mgmt.vm", params, resp.getOutputStream());
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        try {
            String operation = getRequired(req, "operation").toLowerCase();
            if("swap".equals(operation)) {
                doSwap(req, resp);
            } else if("fetch".equals(operation)) {
                doFetch(req, resp);
            } else if("rollback".equals(operation)) {
                doRollback(req);
            } else {
                throw new IllegalArgumentException("Unknown operation parameter: "
                                                   + req.getParameter("operation"));
            }
        } catch(Exception e) {
            logger.error("Error while performing operation.", e);
            resp.sendError(500, "Error while performing operation: " + e.getMessage());
        }
    }

    private void doSwap(HttpServletRequest req, HttpServletResponse resp) throws IOException,
            ServletException {
        String indexFile = getRequired(req, "index");
        String dataFile = getRequired(req, "data");
        String storeName = getRequired(req, "store");
        RandomAccessFileStore store = this.getStore(storeName);
        if(store == null)
            throw new ServletException("'" + storeName + "' is not a registered read-only store.");
        if(!Utils.isReadableFile(indexFile))
            throw new ServletException("Index file '" + indexFile + "' is not a readable file.");
        if(!Utils.isReadableFile(dataFile))
            throw new ServletException("Data file '" + dataFile + "' is not a readable file.");

        store.swapFiles(indexFile, dataFile);
        resp.getWriter().write("Swap completed.");
    }

    private void doFetch(HttpServletRequest req, HttpServletResponse resp) throws IOException,
            ServletException {
        String indexUrl = getRequired(req, "index");
        String dataUrl = getRequired(req, "data");

        // fetch the files if necessary
        File indexFile;
        File dataFile;
        if(fileFetcher == null) {
            indexFile = new File(indexUrl);
            dataFile = new File(dataUrl);
        } else {
            logger.info("Executing fetch of " + dataUrl);
            dataFile = fileFetcher.fetchFile(dataUrl);

            // fetch index second in hope that it will still be in page
            // cache when the swap occurs
            logger.info("Executing fetch of " + indexUrl);
            indexFile = fileFetcher.fetchFile(indexUrl);
            logger.info("Fetch complete.");
        }
        resp.getWriter().write(indexFile.getAbsolutePath());
        resp.getWriter().write("\n");
        resp.getWriter().write(dataFile.getAbsolutePath());
    }

    private void doRollback(HttpServletRequest req) throws ServletException {
        String storeName = getRequired(req, "store");
        RandomAccessFileStore store = getStore(storeName);
        store.rollback();
    }

    private String getRequired(HttpServletRequest req, String name) throws ServletException {
        String val = req.getParameter(name);
        if(val == null)
            throw new ServletException("Missing required parameter '" + name + "'.");
        return val;
    }

    private RandomAccessFileStore getStore(String storeName) throws ServletException {
        for(RandomAccessFileStore store: this.stores)
            if(store.getName().equals(storeName))
                return store;
        throw new ServletException("'" + storeName + "' is not a registered read-only store.");
    }
}
