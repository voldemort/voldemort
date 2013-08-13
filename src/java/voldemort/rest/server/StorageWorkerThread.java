package voldemort.rest.server;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import voldemort.common.VoldemortOpCode;
import voldemort.rest.DeleteResponseSender;
import voldemort.rest.GetAllResponseSender;
import voldemort.rest.GetResponseSender;
import voldemort.rest.GetVersionResponseSender;
import voldemort.rest.PutResponseSender;
import voldemort.rest.RestDeleteErrorHandler;
import voldemort.rest.RestErrorHandler;
import voldemort.rest.RestGetErrorHandler;
import voldemort.rest.RestGetVersionErrorHandler;
import voldemort.rest.RestPutErrorHandler;
import voldemort.store.CompositeVoldemortRequest;
import voldemort.store.Store;
import voldemort.store.stats.StoreStats;
import voldemort.store.stats.StoreStatsJmx;
import voldemort.utils.ByteArray;
import voldemort.utils.JmxUtils;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class StorageWorkerThread implements Runnable {

    private final static RestGetErrorHandler getErrorHandler = new RestGetErrorHandler();
    private final static RestDeleteErrorHandler deleteErrorHandler = new RestDeleteErrorHandler();
    private final static RestGetVersionErrorHandler getVersionErrorHandler = new RestGetVersionErrorHandler();
    private final static RestPutErrorHandler putErrorHandler = new RestPutErrorHandler();
    private ConcurrentHashMap<String, StoreStats> storeStatsMap;
    private StoreStats performanceStats = null, aggregatedStoreStats = null;
    CompositeVoldemortRequest<ByteArray, byte[]> requestObject;
    private MessageEvent messageEvent;
    private Store<ByteArray, byte[], byte[]> store;
    private final int localZoneId;
    private boolean isJmxEnabled = false;

    private final Logger logger = Logger.getLogger(StorageWorkerThread.class);

    public StorageWorkerThread(MessageEvent messageEvent,
                               ConcurrentHashMap<String, StoreStats> storeStatsMap,
                               StoreStats aggregatedStoreStats,
                               boolean isJmxEnabled,
                               int localZoneId) {
        this.messageEvent = messageEvent;
        this.storeStatsMap = storeStatsMap;
        this.aggregatedStoreStats = aggregatedStoreStats;
        this.isJmxEnabled = isJmxEnabled;
        this.localZoneId = localZoneId;
    }

    @Override
    public void run() {
        Object message = messageEvent.getMessage();
        boolean fromLocalZone = false;
        if(message instanceof VoldemortStoreRequest) {
            VoldemortStoreRequest storeRequestObject = (VoldemortStoreRequest) message;
            this.requestObject = storeRequestObject.getRequestObject();
            long now = System.currentTimeMillis();
            if(requestObject.getRequestOriginTimeInMs() + requestObject.getRoutingTimeoutInMs() <= now) {
                RestErrorHandler.writeErrorResponse(messageEvent,
                                                    HttpResponseStatus.REQUEST_TIMEOUT,
                                                    "current time: "
                                                            + now
                                                            + "\torigin time: "
                                                            + requestObject.getRequestOriginTimeInMs()
                                                            + "\ttimeout in ms: "
                                                            + requestObject.getRoutingTimeoutInMs());
                return;
            } else {
                fromLocalZone = isLocalZoneId(storeRequestObject.getZoneId());
                this.store = storeRequestObject.getStore();
                String storeName = store.getName();

                performanceStats = storeStatsMap.get(storeName);
                if(performanceStats == null) {
                    // Add to concurrentHashMap
                    storeStatsMap.putIfAbsent(storeName, new StoreStats(aggregatedStoreStats));
                    performanceStats = storeStatsMap.get(storeName);

                    // Register MBeans for new store performance stats
                    if(isJmxEnabled) {
                        JmxUtils.registerMbean(new StoreStatsJmx(performanceStats),
                                               JmxUtils.createObjectName(JmxUtils.getPackageName(this.getClass())
                                                                                 + ".store.stats",
                                                                         storeName));
                    }
                }

                switch(requestObject.getOperationType()) {
                    case VoldemortOpCode.GET_OP_CODE:
                        if(logger.isDebugEnabled()) {
                            logger.debug("Incoming get request");
                        }
                        try {
                            List<Versioned<byte[]>> versionedValues = store.get(requestObject.getKey(),
                                                                                null);
                            // handle non existing key
                            if(versionedValues.size() > 0) {
                                GetResponseSender responseConstructor = new GetResponseSender(messageEvent,
                                                                                              requestObject.getKey(),
                                                                                              versionedValues,
                                                                                              storeName);
                                responseConstructor.sendResponse(performanceStats,
                                                                 fromLocalZone,
                                                                 requestObject.getRequestOriginTimeInMs());

                            } else {
                                logger.error("Error when doing get. Key does not exist");
                                RestErrorHandler.writeErrorResponse(messageEvent,
                                                                    NOT_FOUND,
                                                                    "Key does not exist");
                            }

                        } catch(Exception e) {
                            getErrorHandler.handleExceptions(messageEvent, e);
                        }
                        break;

                    case VoldemortOpCode.GET_ALL_OP_CODE:
                        if(logger.isDebugEnabled()) {
                            logger.debug("Incoming get all request");
                        }
                        try {
                            Map<ByteArray, List<Versioned<byte[]>>> keyValuesMap = store.getAll(requestObject.getIterableKeys(),
                                                                                                null);
                            // check if there is atleast one valid key
                            // before sending response
                            boolean hasAtleastOneValidKey = false;
                            for(List<Versioned<byte[]>> values: keyValuesMap.values()) {
                                if(values.size() > 0) {
                                    hasAtleastOneValidKey = true;
                                    break;
                                }
                            }
                            if(hasAtleastOneValidKey) {
                                GetAllResponseSender responseConstructor = new GetAllResponseSender(messageEvent,
                                                                                                    keyValuesMap,
                                                                                                    storeName);
                                responseConstructor.sendResponse(performanceStats,
                                                                 fromLocalZone,
                                                                 requestObject.getRequestOriginTimeInMs());
                            } else {
                                logger.error("Error when doing getall. Key does not exist or key is null");
                                RestErrorHandler.writeErrorResponse(messageEvent,
                                                                    NOT_FOUND,
                                                                    "Key does not exist or key is null");
                            }
                        } catch(Exception e) {
                            getErrorHandler.handleExceptions(messageEvent, e);
                        }
                        break;

                    case VoldemortOpCode.PUT_OP_CODE:
                        if(logger.isDebugEnabled()) {
                            logger.debug("Incoming put request");
                        }
                        try {
                            store.put(requestObject.getKey(), requestObject.getValue(), null);
                            PutResponseSender responseConstructor = new PutResponseSender(messageEvent);
                            responseConstructor.sendResponse(performanceStats,
                                                             fromLocalZone,
                                                             requestObject.getRequestOriginTimeInMs());
                        } catch(Exception e) {
                            putErrorHandler.handleExceptions(messageEvent, e);
                        }
                        break;

                    case VoldemortOpCode.DELETE_OP_CODE:
                        if(logger.isDebugEnabled()) {
                            logger.debug("Incoming delete request");
                        }
                        try {
                            boolean result = store.delete(requestObject.getKey(),
                                                          requestObject.getVersion());
                            if(!result) {
                                logger.error("Error when doing delete. Non Existing key/version. Nothing to delete");
                                RestErrorHandler.writeErrorResponse(messageEvent,
                                                                    NOT_FOUND,
                                                                    "Non Existing key/version. Nothing to delete");
                                break;
                            }
                            DeleteResponseSender responseConstructor = new DeleteResponseSender(messageEvent);
                            responseConstructor.sendResponse(performanceStats,
                                                             fromLocalZone,
                                                             requestObject.getRequestOriginTimeInMs());
                        } catch(Exception e) {
                            deleteErrorHandler.handleExceptions(messageEvent, e);
                        }
                        break;

                    case VoldemortOpCode.GET_VERSION_OP_CODE:

                        if(logger.isDebugEnabled()) {
                            logger.debug("Incoming get version request");
                        }
                        try {
                            List<Version> versions = store.getVersions(requestObject.getKey());

                            // handle non existing key
                            if(versions.size() > 0) {
                                GetVersionResponseSender responseConstructor = new GetVersionResponseSender(messageEvent,
                                                                                                            requestObject.getKey(),
                                                                                                            versions,
                                                                                                            storeName);
                                responseConstructor.sendResponse(performanceStats,
                                                                 fromLocalZone,
                                                                 requestObject.getRequestOriginTimeInMs());

                            } else {
                                logger.error("Error when doing getversion. Key does not exist or key is null");
                                RestErrorHandler.writeErrorResponse(messageEvent,
                                                                    NOT_FOUND,
                                                                    "Key does not exist or key is null");
                            }
                        } catch(Exception e) {
                            getVersionErrorHandler.handleExceptions(messageEvent, e);
                        }
                        break;
                    default:
                        // Since we dont add any other operations than the 5
                        // above, the code stops here.
                        return;
                }
            }
        }
    }

    private boolean isLocalZoneId(int requestZoneId) {
        if(requestZoneId == this.localZoneId) {
            return true;
        } else {
            return false;
        }
    }

}
