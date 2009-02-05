package voldemort.contrib.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

public class ContribUtils {

    private static Logger logger = Logger.getLogger(ContribUtils.class);

    public static String getFileFromPathList(Path[] pathList, String fileName) {
        for(Path file: pathList) {
            if(file.getName().equals(fileName)) {
                return file.toUri().getPath();
            }
        }

        logger.warn("File:" + fileName + " not found in given PathList");
        return null;
    }

    public static Cluster getVoldemortClusterDetails(String clusterFile) throws IOException {
        // create voldemort cluster definition using Distributed cache file
        logger.info("Reading cluster details from file:" + clusterFile);
        return new ClusterMapper().readCluster(new BufferedReader(new FileReader(clusterFile),
                                                                  1000000));
    }

    public static StoreDefinition getVoldemortStoreDetails(String storeFile, String storeName)
            throws IOException {
        logger.info("Reading Store details from file:" + storeFile + " for storeName:" + storeName);
        List<StoreDefinition> stores = new StoreDefinitionsMapper().readStoreList(new BufferedReader(new FileReader(storeFile)));
        for(StoreDefinition def: stores) {
            if(def.getName().equals(storeName))
                return def;
        }
        logger.info("Can't Find Store:" + storeName);
        return null;
    }

    public static String getFileFromCache(JobConf conf, String fileName) throws IOException {
        if("local".equals(conf.get("mapred.job.tracker"))) {
            // For local mode Distributed cache is not set.
            // try getting the raw file path.
            URI[] uris = DistributedCache.getCacheFiles(conf);
            Path[] paths = new Path[uris.length];
            int index = 0;
            for(URI uri: uris) {
                logger.info("Adding uri:" + uri);
                if(uri.getFragment().equals(fileName)) {
                    return uri.getPath();
                }
            }
            return getFileFromPathList(paths, fileName);
        } else {
            // For Distributed filesystem.
            Path[] pathList = DistributedCache.getLocalCacheFiles(conf);
            return getFileFromPathList(pathList, fileName);
        }
    }
}
