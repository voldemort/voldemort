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
import voldemort.store.readonly.swapper.AdminStoreSwapper;

import azkaban.jobExecutor.AbstractJob;


public class GobblinDistcpJob extends AbstractJob {
    static String source;
    private final String destination;
    private final Props props;

    GobblinDistcpJob(String id, String source, String destination, Props props) {
        super(id, PrefixedLogger.getLogger(AdminStoreSwapper.class.getName(), destination));
        this.source = source;
        this.destination = destination;
        this.props = props;
    }

    public void run() throws Exception {
        String cdnURL = pickCDN();

        if (!cdnURL.isEmpty()) {
            info("####################################");
            info("              Distcp");
            info("####################################");
            info("Using CDN: " + cdnURL);
            try {
                String username = props.getString("env.USER", "unknownClient");
                String pathPrefix = props.getString(VoldemortBuildAndPushJob.PUSH_CDN_PREFIX, "/tmp/VoldemortBnP/");
                pathPrefix = pathPrefix.endsWith("/") ? pathPrefix : pathPrefix + "/";
                String cdnDir = source.replaceAll(".*://.*?(?=/)", cdnURL + pathPrefix + username);
                FileSystem cdnRootFS = getRootFS(cdnDir);
                Path from = new Path(source);
                Path to = new Path(cdnDir);

                if (!prereqSatisfied(cdnURL)) {
                    warn("\"other_namenodes\" does not contain the CDN cluster address " + cdnURL);
                    warn("The following steps will fail soon, and distcp will be skipped!");
                    warn("Please add/append \"" + cdnURL + "\" to the \"other_namenodes\" attribute in your job specification.");
                }

                deleteDir(cdnRootFS, cdnDir);
                runDistcp(from, to);
                deleteDirOnExit(cdnRootFS, cdnDir);
                addPermissionsToParents(cdnRootFS, cdnDir);
                source = cdnDir;
                info("Use CDN HDFS cluster: " + source);
            } catch (Exception e) {
                warn("An exception occurred during distcp: " + e.getMessage());
                warn("Use original HDFS cluster: " + source);
            }
            info("####################################");
            info("          End of Distcp");
            info("####################################");
        }
    }

    private void runDistcp(Path from, Path to) throws Exception {
        info("source: " + from);
        info("destination: " + to);
        EmbeddedGobblin embeddedGobblin = new EmbeddedGobblinDistcp(from, to).mrMode();

        // Used for global throttling"
        embeddedGobblin.distributeJar("lib/*");

        for (Map.Entry<String, String> entry : this.props.entrySet()) {
            if (entry.getKey() != null && (entry.getKey()).startsWith("distcpConf.")) {
                String key = (entry.getKey()).substring("distcpConf.".length());
                embeddedGobblin.setConfiguration(key, entry.getValue());
            }
        }
        JobExecutionResult result =  embeddedGobblin.run();

        if (!result.isSuccessful()) {
            throw new RuntimeException("Distcp job failed!", result.getErrorCause());
        }
    }

    private FileSystem getRootFS(String target) throws Exception {
        URI rootURI = new URI(target.replaceAll("(?<=:[0-9]{1,5})/.*", ""));
        return FileSystem.get(rootURI, new JobConf());
    }

    private void deleteDir(FileSystem fs, String target) throws Exception {
        Path path = new Path(target.replaceAll(".*://.*?(?=/)", ""));

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
        Path path = new Path(target.replaceAll(".*://.*?(?=/)", ""));

        fs.deleteOnExit(path);  // Delete the directory even if an exception occurs
        info(path + " is scheduled to be deleted on exit.");
    }

    private void addPermissionsToParents(FileSystem fs, String target) throws Exception {
        String pathFromRoot = target.replaceAll(".*://.*?/", "");
        String parent = "";

        for (String seg: pathFromRoot.split("/")) {
            parent = parent + "/" + seg;
            Path parentPath = new Path(parent);
            FsPermission perm = fs.getFileStatus(parentPath).getPermission();
            if (!perm.getOtherAction().implies(FsAction.READ_EXECUTE)) {
                fs.setPermission(parentPath, new FsPermission(perm.getUserAction(), perm.getGroupAction(), perm.getOtherAction().or(FsAction.READ_EXECUTE)));
                info("for path " + parent + ", permissions changed:");
                info("from: " + perm.toString());
                info("  to: " + fs.getFileStatus(parentPath).getPermission().toString());
            }
        }
    }

    private String pickCDN() {
        List<String> pushClusters = props.getList(VoldemortBuildAndPushJob.PUSH_CLUSTER);

        if (!props.containsKey(VoldemortBuildAndPushJob.PUSH_CDN_CLUSTER)) {
            return "";
        }
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

        if (!cdnCluster.matches(".*hdfs://.*:[0-9]{1,5}/?")) {
            warn("Invalid URL format! Will bypass CDN for push cluster " + destination);
            return "";
        }
        return cdnCluster.replaceAll("(?<=:[0-9]{1,5})/", "");
    }

    private boolean prereqSatisfied(String cdnURL) {
        for (String namenode: props.getList("other_namenodes")) {
            if (namenode.replaceAll("(?<=:[0-9]{1,5})/", "").equals(cdnURL)) {
                return true;
            }
        }
        return false;
    }
}
