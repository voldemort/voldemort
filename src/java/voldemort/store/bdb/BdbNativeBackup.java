package voldemort.store.bdb;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.util.DbBackup;
import voldemort.VoldemortException;
import voldemort.server.protocol.admin.AsyncOperationStatus;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Handles native backups for the BDB storage engine
 */
public class BdbNativeBackup {

    public static final String BDB_EXT = ".jdb";

    private final Environment env;
    private final File databaseDir;

    public BdbNativeBackup(Environment env) {
        this.env = env;
        this.databaseDir = env.getHome();
    }

    public void performBackup(File backupDir, AsyncOperationStatus status) {
        // Find the file number of the last file in the previous backup
        // persistently, by either checking the backup archive, or saving
        // state in a persistent file.
        Long lastFileInPrevBackup = determineLastFile(backupDir, status);

        try {
            DbBackup backupHelper;

            if (lastFileInPrevBackup == null) {
                backupHelper = new DbBackup(env);
            } else {
                backupHelper = new DbBackup(env, lastFileInPrevBackup);
            }

            // Start backup, find out what needs to be copied.
            backupHelper.startBackup();
            try {
                String[] filesForBackup = backupHelper.getLogFilesInBackupSet();

                // Copy the files to archival storage.
                backupFiles(filesForBackup, backupDir, status);
            } finally {
                // Remember to exit backup mode, or all log files won't be cleaned
                // and disk usage will bloat.
                backupHelper.endBackup();
            }
        } catch (DatabaseException e) {
            throw new VoldemortException("Error performing native backup", e);
        }
    }

    private Long determineLastFile(File backupDir, final AsyncOperationStatus status) {
        status.setStatus("Determining the last backed up file...");
        File[] backupFiles = backupDir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                if (!name.endsWith(BDB_EXT)) {
                    return false;
                }
                String part = name.substring(0, name.length() - BDB_EXT.length());
                try {
                    Long.parseLong(part);
                } catch (NumberFormatException nfe) {
                    status.setStatus("Warning: " + BDB_EXT + " file whose name is not a number, ignoring: " + name);
                    return false;
                }
                return true;
            }
        });
        if (backupFiles.length == 0) {
            status.setStatus("No backup files found, assuming a full backup is required.");
            return null;
        }
        long largest = Long.MIN_VALUE;
        for (File file : backupFiles) {
            long value = fileNameToNumber(file.getName());
            if (value > largest) {
                largest = value;
            }
        }
        status.setStatus("Last backed up file was " + largest);
        return largest;
    }

    private void backupFiles(String[] filesForBackup, File backupDir, AsyncOperationStatus status) {
        // Determine size of backup
        long size = 0;
        for (String name : filesForBackup) {
            size += new File(databaseDir, name).length();
        }
        status.setStatus(String.format("Backing up %d files with a total of %.1fMB", filesForBackup.length, mb(size)));

        // Ensure files are sorted in order, so that if we fail part way through, we don't lose stuff
        Arrays.sort(filesForBackup, new Comparator<String>() {
            public int compare(String o1, String o2) {
                long result = fileNameToNumber(o1) - fileNameToNumber(o2);
                if (result < 0) {
                    return -1;
                } else if (result > 0) {
                    return 1;
                }
                return 0;
            }
        });
        long total = 0;
        for (String name : filesForBackup) {
            File source = new File(databaseDir, name);
            File dest = new File(backupDir, name);
            status.setStatus(String.format("% 3d%% Copying %s", total * 100 / size, name));
            try {
                copyFile(source, dest);
            } catch (IOException e) {
                // If the destination file exists, delete it
                if (dest.exists()) {
                    dest.delete();
                }
                throw new VoldemortException("Error occured while copying " + name +
                        ". Deleting to ensure we don't have a corrupt backup.", e);
            }
            total += source.length();
        }

    }

    // Fast NIO copy method
    private void copyFile(File sourceFile, File destFile) throws IOException {
        if (!destFile.exists()) {
            destFile.createNewFile();
        }

        FileChannel source = null;
        FileChannel destination = null;
        try {
            source = new FileInputStream(sourceFile).getChannel();
            destination = new FileOutputStream(destFile).getChannel();
            destination.transferFrom(source, 0, source.size());
        } finally {
            if (source != null) {
                source.close();
            }
            if (destination != null) {
                destination.close();
            }
        }
    }

    private static long fileNameToNumber(String name)
    {
        String part = name.substring(0, name.length() - BDB_EXT.length());
        return Long.parseLong(part);
    }

    private static double mb(long value) {
        return ((double) value) / 1048576;
    }

}
