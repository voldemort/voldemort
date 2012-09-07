package voldemort.store.readonly.mr.azkaban;

import java.util.Properties;

/**
 * This interface defines a Raw Job interface. Each job defines
 * <ul>
 * <li>Job Type : {HADOOP, UNIX, JAVA, SUCCESS_TEST, CONTROLLER}</li>
 * <li>Job ID/Name : {String}</li>
 * <li>Arguments: Key/Value Map for Strings</li>
 * </ul>
 * 
 * A job is required to have a constructor Job(String jobId, Props props)
 */

public interface Job {

    /**
     * Returns a unique(should be checked in xml) string name/id for the Job.
     * 
     * @return
     */
    public String getId();

    /**
     * Run the job. In general this method can only be run once. Must either
     * succeed or throw an exception.
     */
    public void run() throws Exception;

    /**
     * Best effort attempt to cancel the job.
     * 
     * @throws Exception If cancel fails
     */
    public void cancel() throws Exception;

    /**
     * Returns a progress report between [0 - 1.0] to indicate the percentage
     * complete
     * 
     * @throws Exception If getting progress fails
     */
    public double getProgress() throws Exception;

    /**
     * Get the generated properties from this job.
     * 
     * @return
     */
    public Properties getJobGeneratedProperties();

    /**
     * Determine if the job was cancelled.
     * 
     * @return
     */
    public boolean isCanceled();
}