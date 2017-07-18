package voldemort.store.readonly.mr.azkaban;

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
    private String source;
    private final String destination;
    private final Props props;

    GobblinDistcpJob(String id, String sourceHdfsCluster, String destinationVoldemortCluster, Props props) {
        super(id, PrefixedLogger.getLogger(GobblinDistcpJob.class.getName(), destinationVoldemortCluster));
        this.source = sourceHdfsCluster;
        this.destination = destinationVoldemortCluster;
        this.props = props;
    }

    public void run() throws Exception {
        info("###############  Distcp  ###############");
        try {
            String cdnURL = pickCDN();
            if (!cdnURL.isEmpty()) {
                info("Using CDN: " + cdnURL);
                String username = props.getString("env.USER", "unknownUser");
                String pathPrefix = props.getString(VoldemortBuildAndPushJob.PUSH_CDN_PREFIX).trim();
                assert pathPrefix.startsWith("/");
                pathPrefix = removeTrailingSlash(pathPrefix);
                // Replace original cluster with CDN, e.g. hdfs://original:9000/a/b/c => webhdfs://cdn:50070/prefix/user/a/b/c
                String cdnDir =  cdnURL + pathPrefix + "/" + username + extractPathFromUrl(source);
                FileSystem cdnRootFS = getRootFS(cdnDir);
                Path from = new Path(source);
                Path to = new Path(cdnDir);

                if (!prereqSatisfied(cdnURL)) {
                    warn("Please add/append \"" + cdnURL + "\" to the \"other_namenodes\" attribute in your job specification.");
                    throw new RuntimeException("\"other_namenodes\" does not contain the CDN cluster address " + cdnURL);
                }

                deleteDir(cdnRootFS, cdnDir);
                runDistcp(from, to);
                deleteDirOnExit(cdnRootFS, cdnDir);
                addPermissionsToParents(cdnRootFS, cdnDir);
                source = cdnDir;
                info("Use CDN HDFS cluster: " + source);
            }
        } catch (Exception e) {
            warn("An exception occurred during distcp: " + e.getMessage());
            e.printStackTrace();
            warn("Use original HDFS cluster: " + source);
        }
        info("############  End of Distcp  ###########");
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

    private FileSystem getRootFS(String target) throws Exception {
        URI rootURI = new URI(removePathFromUrl(target));
        return FileSystem.get(rootURI, new JobConf());
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

    private void addPermissionsToParents(FileSystem fs, String target) throws Exception {
        String pathFromRoot = removeLeadingSlash(extractPathFromUrl(target));
        String parent = "";

        for (String seg: pathFromRoot.split("/")) {
            parent = parent + "/" + seg;
            Path parentPath = new Path(parent);
            FsPermission perm = fs.getFileStatus(parentPath).getPermission();
            FsAction u = perm.getUserAction();
            FsAction g = perm.getGroupAction();
            FsAction o = perm.getOtherAction();
            boolean changed = false;

            if (props.getBoolean(VoldemortBuildAndPushJob.PUSH_CDN_READ_BY_GROUP, false)) {
                if (!g.implies(FsAction.READ_EXECUTE)) {
                    g = g.or(FsAction.READ_EXECUTE);
                    changed = true;
                }
            }

            if (props.getBoolean(VoldemortBuildAndPushJob.PUSH_CDN_READ_BY_OTHER, false)) {
                if (!o.implies(FsAction.READ_EXECUTE)) {
                    o = o.or(FsAction.READ_EXECUTE);
                    changed = true;
                }
            }

            if (changed) {
                FsPermission desiredPerm = new FsPermission(u, g, o);
                fs.setPermission(parentPath, desiredPerm);
                assert fs.getFileStatus(parentPath).getPermission().equals(desiredPerm);
                info("for path " + parent + ", permissions changed from: " + perm + " to: " + desiredPerm);
            }
        }
    }

    private String pickCDN() throws Exception {
        List<String> pushClusters = props.getList(VoldemortBuildAndPushJob.PUSH_CLUSTER);
        List<String> cdnClusters = props.getList(VoldemortBuildAndPushJob.PUSH_CDN_CLUSTER);

        if (pushClusters.size() != cdnClusters.size()) {
            warn("Cluster sizes are different! Will bypass CDN for push cluster " + destination);
            return "";
        }

        int index = pushClusters.indexOf(destination);
        assert index != -1;
        String cdnCluster = cdnClusters.get(index);
        if (cdnCluster.equals("null")) {
            info("Will bypass CDN for push cluster " + destination);
            return "";
        }

        if (!cdnCluster.matches(".*hdfs://.+:[0-9]{1,5}/?")) {
            warn("Invalid URL format! Will bypass CDN for push cluster " + destination);
            return "";
        }
        return removeTrailingSlash(cdnCluster);
    }

    private boolean prereqSatisfied(String cdnURL) throws Exception {
        for (String namenode: props.getList("other_namenodes")) {
            if (removeTrailingSlash(namenode).equals(cdnURL)) {
                return true;
            }
        }
        return false;
    }

    public String getSource() {
        return source;
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
        assert path.startsWith("/");
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
}
