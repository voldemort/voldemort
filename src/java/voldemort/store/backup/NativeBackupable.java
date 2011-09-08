package voldemort.store.backup;

import voldemort.server.protocol.admin.AsyncOperationStatus;

import java.io.File;

/**
 * Storage engines that support native backups, that is, efficient backups to a locally mounted directory that ensure
 * the integrity of the store, may implement this.
 */
public interface NativeBackupable {

    /**
     * Perform a native backup
     *
     * @param toDir    The directory to backup to
     * @param status   The async operation status to update/check
     */
    public void nativeBackup(File toDir, AsyncOperationStatus status);
}
