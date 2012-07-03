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

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.common.service.ServiceType;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.server.http.VoldemortServletContextListener;
import voldemort.server.storage.StorageService;
import voldemort.store.StorageEngine;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.readonly.FileFetcher;
import voldemort.store.readonly.ReadOnlyStorageEngine;
import voldemort.store.readonly.ReadOnlyUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.ReflectUtils;
import voldemort.utils.Utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * A servlet that supports both manual or programatic operations on read only
 * stores. The operations are
 * <ol>
 * <li>FETCH. Fetch the given files to the local node. Parameters:
 * operation="fetch", dir=[data-directory], store=[name-of-store],
 * pushVersion=[version-of-push]</li>
 * <li>SWAP. Swap the data directory atomically. Parameters: operation="swap",
 * store=[name-of-store]</li>
 * <li>ROLLBACK. Rollback the store to previous push version. Parameters:
 * operation="rollback", store=[name-of-store],
 * pushVersion=[version-of-push-to-rollback-to]</li>
 * </ol>
 * 
 * 
 */
public class ReadOnlyStoreManagementServlet extends HttpServlet {

    private static final long serialVersionUID = 1;
    private static final Logger logger = Logger.getLogger(ReadOnlyStoreManagementServlet.class);

    private volatile List<ReadOnlyStorageEngine> stores;
    private VelocityEngine velocityEngine;
    private FileFetcher fileFetcher;
    private MetadataStore metadataStore;

    public ReadOnlyStoreManagementServlet() {}

    public ReadOnlyStoreManagementServlet(VoldemortServer server, VelocityEngine engine) {
        this.stores = getReadOnlyStores(server);
        this.velocityEngine = Utils.notNull(engine);
        setFetcherClass(server);
    }

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        VoldemortServer server = (VoldemortServer) config.getServletContext()
                                                         .getAttribute(VoldemortServletContextListener.SERVER_KEY);

        initMetadataStore(server);
        initStores(server);
        initVelocity(config);
        setFetcherClass(server);
    }

    public void initMetadataStore(VoldemortServer server) {
        this.metadataStore = Utils.notNull(server).getMetadataStore();
    }

    public void initStores(VoldemortServer server) {
        this.stores = getReadOnlyStores(server);
    }

    public void initVelocity(ServletConfig config) {
        this.velocityEngine = (VelocityEngine) Utils.notNull(config.getServletContext()
                                                                   .getAttribute(VoldemortServletContextListener.VELOCITY_ENGINE_KEY));
    }

    private void setFetcherClass(VoldemortServer server) {
        String className = server.getVoldemortConfig()
                                 .getAllProps()
                                 .getString("file.fetcher.class", null);
        if(className == null || className.trim().length() == 0) {
            this.fileFetcher = null;
        } else {
            try {
                logger.info("Loading fetcher " + className);
                Class<?> cls = Class.forName(className.trim());
                this.fileFetcher = (FileFetcher) ReflectUtils.callConstructor(cls,
                                                                              new Class<?>[] { VoldemortConfig.class },
                                                                              new Object[] { server.getVoldemortConfig() });
            } catch(Exception e) {
                throw new VoldemortException("Error loading file fetcher class " + className, e);
            }
        }
    }

    private List<ReadOnlyStorageEngine> getReadOnlyStores(VoldemortServer server) {
        StorageService storage = (StorageService) Utils.notNull(server)
                                                       .getService(ServiceType.STORAGE);
        List<ReadOnlyStorageEngine> l = Lists.newArrayList();
        for(StorageEngine<ByteArray, byte[], byte[]> engine: storage.getStoreRepository()
                                                                    .getStorageEnginesByClass(ReadOnlyStorageEngine.class)) {
            l.add((ReadOnlyStorageEngine) engine);
        }
        return l;
    }

    @Override
    public void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
            IOException {
        Map<String, Object> params = Maps.newHashMap();
        params.put("stores", stores);
        velocityEngine.render("read-only-mgmt.vm", params, resp.getOutputStream());
    }

    @Override
    public void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
            IOException {
        try {
            String operation = getRequired(req, "operation").toLowerCase();
            if("swap".equals(operation)) {
                doSwap(req, resp);
            } else if("fetch".equals(operation)) {
                doFetch(req, resp);
            } else if("rollback".equals(operation)) {
                doRollback(req);
            } else if("failed-fetch".equals(operation)) {
                doFailedFetch(req);
            } else {
                throw new IllegalArgumentException("Unknown operation parameter: "
                                                   + req.getParameter("operation"));
            }
        } catch(Exception e) {
            logger.error("Error while performing operation.", e);
            resp.sendError(500, "Error while performing operation: " + e.getMessage());
        }
    }

    private void doFailedFetch(HttpServletRequest req) throws ServletException {
        String dir = getRequired(req, "dir");
        String storeName = getRequired(req, "store");

        try {

            if(!Utils.isReadableDir(dir))
                throw new ServletException("Could not read folder " + dir
                                           + " correctly to delete it");

            ReadOnlyStorageEngine store = this.getStore(storeName);
            if(store.getCurrentVersionId() == ReadOnlyUtils.getVersionId(new File(dir))) {
                logger.warn("Cannot delete " + dir + " for " + storeName
                            + " since it is the current dir");
                return;
            }

            Utils.rm(new File(dir));
        } catch(Exception e) {
            throw new ServletException(e);
        }
    }

    private void doSwap(HttpServletRequest req, HttpServletResponse resp) throws IOException,
            ServletException {
        String dir = getRequired(req, "dir");
        String storeName = getRequired(req, "store");

        if(metadataStore != null
           && !metadataStore.getServerState().equals(MetadataStore.VoldemortState.NORMAL_SERVER)) {
            throw new ServletException("Voldemort server not in normal state");
        }

        ReadOnlyStorageEngine store = this.getStore(storeName);
        if(store == null)
            throw new ServletException("'" + storeName + "' is not a registered read-only store.");

        if(!Utils.isReadableDir(dir))
            throw new ServletException("Store directory '" + dir + "' is not a readable directory.");

        // Retrieve the current directory before swapping it
        String currentDirPath = store.getCurrentDirPath();

        // Swap with the new directory
        store.swapFiles(dir);

        // Send back the previous directory
        resp.getWriter().write(currentDirPath);
    }

    private void doFetch(HttpServletRequest req, HttpServletResponse resp) throws IOException,
            ServletException {
        String fetchUrl = getRequired(req, "dir");
        String storeName = getRequired(req, "store");
        String pushVersionString = getOptional(req, "pushVersion");

        ReadOnlyStorageEngine store = this.getStore(storeName);
        if(store == null)
            throw new ServletException("'" + storeName + "' is not a registered read-only store.");

        long pushVersion;
        if(pushVersionString == null) {
            // Find the max version
            long maxVersion;
            File[] storeDirList = ReadOnlyUtils.getVersionDirs(new File(store.getStoreDirPath()));
            if(storeDirList == null || storeDirList.length == 0) {
                throw new ServletException("Push version required since no version folders exist");
            } else {
                maxVersion = ReadOnlyUtils.getVersionId(ReadOnlyUtils.findKthVersionedDir(storeDirList,
                                                                                          storeDirList.length - 1,
                                                                                          storeDirList.length - 1)[0]);
            }
            pushVersion = maxVersion + 1;
        } else {
            pushVersion = Long.parseLong(pushVersionString);
            if(pushVersion <= store.getCurrentVersionId())
                throw new ServletException("Version of push specified (" + pushVersion
                                           + ") should be greater than current version "
                                           + store.getCurrentVersionId());
        }

        // fetch the files if necessary
        File fetchDir = null;
        if(fileFetcher == null) {

            logger.warn("File fetcher class has not instantiated correctly. Assuming local file");

            if(!Utils.isReadableDir(fetchUrl)) {
                throw new ServletException("Fetch url " + fetchUrl + " is not readable");
            }

            fetchDir = new File(store.getStoreDirPath(), "version-" + Long.toString(pushVersion));

            if(fetchDir.exists())
                throw new ServletException("Version directory " + fetchDir.getAbsolutePath()
                                           + " already exists");

            Utils.move(new File(fetchUrl), fetchDir);

        } else {
            logger.info("Executing fetch of " + fetchUrl);

            try {
                fetchDir = fileFetcher.fetch(fetchUrl, store.getStoreDirPath() + File.separator
                                                       + "version-" + Long.toString(pushVersion));

                if(fetchDir == null) {
                    throw new ServletException("File fetcher failed for " + fetchUrl
                                               + " and store name = " + storeName
                                               + " due to incorrect input path/checksum error");
                } else {
                    logger.info("Fetch complete.");
                }

            } catch(Exception e) {
                throw new ServletException("Exception in Fetcher = " + e.getMessage());
            }

        }
        resp.getWriter().write(fetchDir.getAbsolutePath());
    }

    private void doRollback(HttpServletRequest req) throws ServletException {
        String storeName = getRequired(req, "store");
        long pushVersion = Long.parseLong(getRequired(req, "pushVersion"));

        ReadOnlyStorageEngine store = getStore(storeName);
        if(store == null)
            throw new ServletException("'" + storeName + "' is not a registered read-only store.");

        try {
            File rollbackVersionDir = new File(store.getStoreDirPath(), "version-" + pushVersion);

            store.rollback(rollbackVersionDir);
        } catch(Exception e) {
            throw new ServletException("Exception in rollback = " + e.getMessage());
        }
    }

    private String getOptional(HttpServletRequest req, String name) {
        return req.getParameter(name);
    }

    private String getRequired(HttpServletRequest req, String name) throws ServletException {
        String val = req.getParameter(name);
        if(val == null)
            throw new ServletException("Missing required parameter '" + name + "'.");
        return val;
    }

    private ReadOnlyStorageEngine getStore(String storeName) throws ServletException {
        for(ReadOnlyStorageEngine store: this.stores)
            if(store.getName().equals(storeName))
                return store;
        throw new ServletException("'" + storeName + "' is not a registered read-only store.");
    }
}
