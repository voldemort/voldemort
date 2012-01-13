package voldemort.store.backup;

import java.io.File;

import voldemort.server.protocol.admin.AsyncOperationStatus;

/**
 * Storage engines that support native backups, that is, efficient backups to a
 * locally mounted directory that ensure the integrity of the store, may
 * implement this.
 */
public interface NativeBackupable {

    /**
     * Perform a native backup
     * 
     * @param toDir The directory to backup to
     * @param checkIntegrity whether the integrity of the copied data needs to
     *        be verified
     * @param isIncremental is the backup incremental
     * @param status The async operation status to update/check
     */
    public void nativeBackup(File toDir,
                             boolean checkIntegrity,
                             boolean isIncremental,
                             AsyncOperationStatus status);
}
