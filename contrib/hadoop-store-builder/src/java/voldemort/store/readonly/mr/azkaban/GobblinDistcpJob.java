package voldemort.store.readonly.mr.azkaban;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.JobConf;

import gobblin.runtime.api.JobExecutionResult;
import gobblin.runtime.embedded.EmbeddedGobblin;
import gobblin.runtime.embedded.EmbeddedGobblinDistcp;

import voldemort.utils.Props;
import voldemort.utils.logging.PrefixedLogger;

import azkaban.jobExecutor.AbstractJob;


public class GobblinDistcpJob extends AbstractJob {
    private final static String ATTR_PREFIX = "distcpConf.";
    private final static String OTHER_NAMENODES = "other_namenodes";
    private String source;
    private final String destination;
    private final Props props;
    private FileSystem cdnTargetFS;

    GobblinDistcpJob(String id, String sourceHdfsCluster, String destinationVoldemortCluster, Props props) {
        super(id, PrefixedLogger.getLogger(GobblinDistcpJob.class.getName(), destinationVoldemortCluster));
        this.source = sourceHdfsCluster;
        this.destination = destinationVoldemortCluster;
        this.props = props;
    }

    public void run() throws Exception {
        info("###############  Distcp  ###############");
        long startTime = System.currentTimeMillis();
        try {
            String cdnURL = pickCDN();
            if (!cdnURL.isEmpty()) {
                info("CDN: " + cdnURL);
                String storeName = props.getString(VoldemortBuildAndPushJob.PUSH_STORE_NAME).trim();
                String pathPrefix = props.getString(VoldemortBuildAndPushJob.PUSH_CDN_PREFIX).trim();
                if (!pathPrefix.startsWith("/")) {
                    throw new RuntimeException(VoldemortBuildAndPushJob.PUSH_CDN_PREFIX + " must start with '/': " + pathPrefix);
                }
                pathPrefix = removeTrailingSlash(pathPrefix);

                // Replace original cluster with CDN, e.g. hdfs://original:9000/a/b/c => webhdfs://cdn:50070/prefix/user/a/b/c
                String cdnDir =  cdnURL + pathPrefix + "/" + storeName + extractPathFromUrl(source);
                cdnTargetFS = getTargetFS(cdnDir);
                Path from = new Path(source);
                Path to = new Path(cdnDir);

                if (!prereqSatisfied(cdnURL)) {
                    warn("Please add/append \"" + cdnURL + "\" to the \"" + OTHER_NAMENODES + "\" attribute in your job specification.");
                    throw new RuntimeException("\"" + OTHER_NAMENODES + "\" does not contain the CDN cluster address " + cdnURL);
                }

                deleteDir(cdnTargetFS, cdnDir);
                runDistcp(from, to);
                deleteDirOnExit(cdnTargetFS, cdnDir);
                addPermissionsToParentDirs(cdnTargetFS, cdnDir, pathPrefix);
                source = cdnDir;
                info("Will use data on CDN HDFS cluster: " + source);
            }
        } catch (Exception e) {
            warn("An exception occurred during distcp: ", e);
            warn("Will use data on original HDFS cluster: " + source);
            long duration = System.currentTimeMillis() - startTime;
            info("#############  End of Distcp (FAILED) ############ (duration: " + duration/1000 + "s)");
            throw new RuntimeException("An exception occurred during distcp", e);
        }
        long duration = System.currentTimeMillis() - startTime;
        info("#############  End of Distcp  ########### (duration: " + duration/1000 + "s)");
    }

    private void runDistcp(Path from, Path to) throws Exception {
        info("sourcePath: " + from + ", destinationPath: " + to);
        EmbeddedGobblin embeddedGobblin = new EmbeddedGobblinDistcp(from, to).mrMode();

        // Used for global throttling"
        embeddedGobblin.distributeJar("lib/*");

        for (Map.Entry<String, String> entry : this.props.entrySet()) {
            if (entry.getKey() != null && (entry.getKey()).startsWith(ATTR_PREFIX)) {
                String key = (entry.getKey()).substring(ATTR_PREFIX.length());
                embeddedGobblin.setConfiguration(key, entry.getValue());
            }
        }
        JobExecutionResult result =  embeddedGobblin.run();

        if (!result.isSuccessful()) {
            throw new RuntimeException("Distcp job failed!", result.getErrorCause());
        }
    }

    private FileSystem getTargetFS(String target) throws Exception {
        URI targetURI = new URI(removePathFromUrl(target));
        return FileSystem.get(targetURI, new JobConf());
    }

    private void deleteDir(FileSystem fs, String target) throws Exception {
        Path path = new Path(extractPathFromUrl(target));

        if (fs.exists(path)) {
            fs.delete(path, true);
            if (fs.exists(path)) {
                warn("Could not delete temp directory " + path + " in CDN!");
            } else {
                info("Deleted " + path);
            }
        }
    }

    private void deleteDirOnExit(FileSystem fs, String target) throws Exception {
        Path path = new Path(extractPathFromUrl(target));

        fs.deleteOnExit(path);  // Delete the directory even if an exception occurs
        info(path + " is scheduled to be deleted on exit.");
    }

    private void checkPermission(FileSystem fs, Path path, boolean checkWritePermission) throws Exception {
        FsPermission perm = fs.getFileStatus(path).getPermission();
        FsAction u = perm.getUserAction();
        FsAction g = perm.getGroupAction();
        FsAction o = perm.getOtherAction();
        boolean changed = false;

        // Check read permission
        if (props.getBoolean(VoldemortBuildAndPushJob.PUSH_CDN_READ_BY_GROUP, true)) {
            if (!g.implies(FsAction.READ_EXECUTE)) {
                g = g.or(FsAction.READ_EXECUTE);
                changed = true;
            }
        }

        if (props.getBoolean(VoldemortBuildAndPushJob.PUSH_CDN_READ_BY_OTHER, true)) {
            if (!o.implies(FsAction.READ_EXECUTE)) {
                o = o.or(FsAction.READ_EXECUTE);
                changed = true;
            }
        }

        // Check write permission
        if (checkWritePermission) {
            if (props.getBoolean(VoldemortBuildAndPushJob.PUSH_CDN_WRITTEN_BY_GROUP, true)) {
                if (!g.implies(FsAction.WRITE)) {
                    g = g.or(FsAction.WRITE);
                    changed = true;
                }
            }

            if (props.getBoolean(VoldemortBuildAndPushJob.PUSH_CDN_WRITTEN_BY_OTHER, true)) {
                if (!o.implies(FsAction.WRITE)) {
                    o = o.or(FsAction.WRITE);
                    changed = true;
                }
            }
        }

        if (changed) {
            FsPermission desiredPerm = new FsPermission(u, g, o);
            fs.setPermission(path, desiredPerm);
            if (!fs.getFileStatus(path).getPermission().equals(desiredPerm)) {
                throw new RuntimeException("Failed to set permission for " + path + " from: " + perm + " to: " + desiredPerm);
            }
            info("for path " + path + ", permissions changed from: " + perm + " to: " + desiredPerm);
        }
    }

    private void addPermissionsToParentDirs(FileSystem fs, String target, String pathPrefix) throws Exception {
        String pathFromRoot = removeLeadingSlash(extractPathFromUrl(target));
        String parent = "";

        for (String seg: pathFromRoot.split("/")) {
            parent = parent + "/" + seg;
            Path parentPath = new Path(parent);

            if (parent.length() <= pathPrefix.length()) {
                checkPermission(fs, parentPath, false);
            } else {
                checkPermission(fs, parentPath, true);
            }
        }
    }

    private Map<String, String> buildCdnMap() throws Exception {
        List<String> pairs = props.getList(VoldemortBuildAndPushJob.PUSH_CDN_CLUSTER);
        Map<String, String> cdnMap = new HashMap<>(pairs.size());

        for (String pair : pairs) {
            if (!pair.contains("|")) {
                throw new RuntimeException("Cannot find separator '|' in K-V pair \"" + pair + "\"");
            }
            String[] urls = pair.split("\\|");
            if (urls.length != 2) {
                throw new RuntimeException("More than one separator found in K-V pair \"" + pair + "\"");
            }
            cdnMap.put(removeTrailingSlash(urls[0].trim()), removeTrailingSlash(urls[1].trim()));
        }
        return cdnMap;
    }

    private String pickCDN() throws Exception {
        String cdnCluster = buildCdnMap().get(removeTrailingSlash(destination));

        if (cdnCluster == null) {
            warn("Cannot find corresponding CDN! Will bypass CDN for push cluster " + destination);
            return "";
        }

        if (cdnCluster.equals("null")) {
            info("Will bypass CDN for push cluster " + destination);
            return "";
        }

        if (!cdnCluster.matches(".*hdfs://.+:[0-9]{1,5}")) {
            warn("Invalid URL format! Will bypass CDN for push cluster " + destination);
            return "";
        }
        return cdnCluster;
    }

    private boolean prereqSatisfied(String cdnURL) throws Exception {
        for (String namenode: props.getList(OTHER_NAMENODES)) {
            if (removeTrailingSlash(namenode).equals(cdnURL)) {
                return true;
            }
        }
        return false;
    }

    private String removeLeadingSlash(String s) {
        return s.replaceAll("^/", "");
    }

    private String removeTrailingSlash(String s) {
        return s.replaceAll("/$", "");
    }

    /**
     * Extract directory path from URL, e.g. hdfs://hostname:9000/a/b/c => /a/b/c
     */
    private String extractPathFromUrl(String url) {
        String path = url.replaceAll(".+://.+?(?=/)", "");
        if (!path.startsWith("/")) {
            throw new RuntimeException("Path must start with '/': " + path);
        }
        return path;
    }

    /**
     * Remove directory path from URL,  e.g. hdfs://hostname:9000/a/b/c => hdfs://hostname:9000
     */
    private String removePathFromUrl(String url) {
        String path = extractPathFromUrl(url);
        int end = url.indexOf(path);
        return url.substring(0, end);

        // Alternative: return url.replaceAll("(?<=:[0-9]{1,5})/.*", "")), assume url contains port number
    }

    public String getSource() {
        return source;
    }

    void closeCdnFS() {
        try {
            if (cdnTargetFS != null) {
                cdnTargetFS.close();
            }
        } catch (Exception e) {
            warn("Failed to close CDN filesystem!");
        }
    }
}
