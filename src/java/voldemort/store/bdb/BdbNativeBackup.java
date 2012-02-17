package voldemort.store.bdb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;

import voldemort.VoldemortException;
import voldemort.server.protocol.admin.AsyncOperationStatus;

import com.sleepycat.je.Environment;
import com.sleepycat.je.util.DbBackup;
import com.sleepycat.je.util.LogVerificationInputStream;

/**
 * Handles native backups for the BDB storage engine
 */
public class BdbNativeBackup {

    private static final String BDB_EXT = ".jdb";
    private static final int LOGVERIFY_BUFSIZE = 1024;

    private final Environment env;
    private final File databaseDir;

    private final boolean verifyFiles;
    private final boolean isIncremental;
    private DbBackup backupHelper;

    public BdbNativeBackup(Environment env, boolean verifyFiles, boolean isIncremental) {
        this.env = env;
        this.verifyFiles = verifyFiles;
        this.isIncremental = isIncremental;
        this.databaseDir = env.getHome();
    }

    public void performBackup(File backupDir, AsyncOperationStatus status) {
        // Find the file number of the last file in the previous backup
        // persistently, by either checking the backup archive, or saving
        // state in a persistent file.
        Long lastFileInPrevBackup = determineLastFile(backupDir, status);

        try {

            if(lastFileInPrevBackup == null) {
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
                // Remember to exit backup mode, or all log files won't be
                // cleaned and disk usage will bloat.
                backupHelper.endBackup();
            }
        } catch(Exception e) {
            throw new VoldemortException("Error performing native backup", e);
        }
    }

    private Long determineLastFile(File backupDir, final AsyncOperationStatus status) {
        status.setStatus("Determining the last backed up file...");
        File[] backupFiles = backupDir.listFiles(new FilenameFilter() {

            public boolean accept(File dir, String name) {
                if(!name.endsWith(BDB_EXT)) {
                    return false;
                }
                String part = name.substring(0, name.length() - BDB_EXT.length());
                try {
                    Long.parseLong(part, 16);
                } catch(NumberFormatException nfe) {
                    status.setStatus("Warning: " + BDB_EXT
                                     + " file whose name is not a number, ignoring: " + name);
                    return false;
                }
                return true;
            }
        });
        if(backupFiles.length == 0) {
            status.setStatus("No backup files found, assuming a full backup is required.");
            return null;
        }
        long largest = Long.MIN_VALUE;
        for(File file: backupFiles) {
            long value = fileNameToNumber(file.getName());
            if(value > largest) {
                largest = value;
            }
        }
        status.setStatus("Last backed up file was " + largest);
        return largest;
    }

    private void backupFiles(String[] filesForBackup, File backupDir, AsyncOperationStatus status) {
        // Determine size of backup
        long size = 0;
        for(String name: filesForBackup) {
            size += new File(databaseDir, name).length();
        }
        status.setStatus(String.format("Backing up %d files with a total of %.1fMB",
                                       filesForBackup.length,
                                       mb(size)));

        // Ensure files are sorted in order, so that if we fail part way
        // through, we don't lose stuff
        Arrays.sort(filesForBackup, new Comparator<String>() {

            public int compare(String o1, String o2) {
                long result = fileNameToNumber(o1) - fileNameToNumber(o2);
                if(result < 0) {
                    return -1;
                } else if(result > 0) {
                    return 1;
                }
                return 0;
            }
        });
        long total = 0;
        for(String name: filesForBackup) {
            File source = new File(databaseDir, name);
            File dest = new File(backupDir, name);
            status.setStatus(String.format("% 3d%% Copying %s", total * 100 / size, name));
            try {
                if(verifyFiles) {
                    verifiedCopyFile(source, dest);
                } else {
                    copyFile(source, dest);
                }
            } catch(IOException e) {
                // If the destination file exists, delete it
                if(dest.exists()) {
                    dest.delete();
                }
                throw new VoldemortException("Error occured while copying "
                                                     + name
                                                     + ". Deleting to ensure we don't have a corrupt backup.",
                                             e);
            }
            total += source.length();
        }

        if(isIncremental) {
            try {
                recordBackupSet(backupDir);
            } catch(IOException e) {
                throw new VoldemortException("Error attempting to write backup records for ", e);
            }
        } else {
            cleanStaleFiles(backupDir, status);
        }
    }

    /**
     * Records the list of backedup files into a text file
     * 
     * @param filesInEnv
     * @param backupDir
     */
    private void recordBackupSet(File backupDir) throws IOException {
        String[] filesInEnv = env.getHome().list();
        SimpleDateFormat format = new SimpleDateFormat("yyyy_MM_dd_kk_mm_ss");
        String recordFileName = "backupset-" + format.format(new Date());
        File recordFile = new File(backupDir, recordFileName);
        if(recordFile.exists()) {
            recordFile.renameTo(new File(backupDir, recordFileName + ".old"));
        }

        PrintStream backupRecord = new PrintStream(new FileOutputStream(recordFile));
        backupRecord.println("Lastfile:" + Long.toHexString(backupHelper.getLastFileInBackupSet()));
        if(filesInEnv != null) {
            for(String file: filesInEnv) {
                if(file.endsWith(BDB_EXT))
                    backupRecord.println(file);
            }
        }
        backupRecord.close();
    }

    /**
     * For recovery from the latest consistent snapshot, we should clean up the
     * old files from the previous backup set, else we will fill the disk with
     * useless log files
     * 
     * @param backupDir
     */
    private void cleanStaleFiles(File backupDir, AsyncOperationStatus status) {
        String[] filesInEnv = env.getHome().list();
        String[] filesInBackupDir = backupDir.list();
        if(filesInEnv != null && filesInBackupDir != null) {
            HashSet<String> envFileSet = new HashSet<String>();
            for(String file: filesInEnv)
                envFileSet.add(file);
            // delete all files in backup which are currently not in environment
            for(String file: filesInBackupDir) {
                if(file.endsWith(BDB_EXT) && !envFileSet.contains(file)) {
                    status.setStatus("Deleting stale jdb file :" + file);
                    File staleJdbFile = new File(backupDir, file);
                    staleJdbFile.delete();
                }
            }
        }
    }

    /**
     * File copy using fast NIO zero copy method
     * 
     * @param sourceFile
     * @param destFile
     * @throws IOException
     */
    private void copyFile(File sourceFile, File destFile) throws IOException {
        if(!destFile.exists()) {
            destFile.createNewFile();
        }

        FileChannel source = null;
        FileChannel destination = null;
        try {
            source = new FileInputStream(sourceFile).getChannel();
            destination = new FileOutputStream(destFile).getChannel();
            destination.transferFrom(source, 0, source.size());
        } finally {
            if(source != null) {
                source.close();
            }
            if(destination != null) {
                destination.close();
            }
        }
    }

    /**
     * Copies the jdb log files, with additional verification of the checksums.
     * 
     * @param sourceFile
     * @param destFile
     * @throws IOException
     */
    private void verifiedCopyFile(File sourceFile, File destFile) throws IOException {
        if(!destFile.exists()) {
            destFile.createNewFile();
        }

        FileInputStream source = null;
        FileOutputStream destination = null;
        LogVerificationInputStream verifyStream = null;
        try {
            source = new FileInputStream(sourceFile);
            destination = new FileOutputStream(destFile);
            verifyStream = new LogVerificationInputStream(env, source, sourceFile.getName());

            final byte[] buf = new byte[LOGVERIFY_BUFSIZE];

            while(true) {
                final int len = verifyStream.read(buf);
                if(len < 0) {
                    break;
                }
                destination.write(buf, 0, len);
            }

        } finally {
            if(verifyStream != null) {
                verifyStream.close();
            }
            if(destination != null) {
                destination.close();
            }
        }
    }

    private static long fileNameToNumber(String name) {
        String part = name.substring(0, name.length() - BDB_EXT.length());
        return Long.parseLong(part, 16);
    }

    private static double mb(long value) {
        return ((double) value) / 1048576;
    }

}
