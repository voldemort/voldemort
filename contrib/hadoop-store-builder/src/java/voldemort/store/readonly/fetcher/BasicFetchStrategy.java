package voldemort.store.readonly.fetcher;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import voldemort.server.protocol.admin.AsyncOperationStatus;
import voldemort.store.readonly.checksum.CheckSum;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;

public class BasicFetchStrategy implements FetchStrategy {

	private final FileSystem fs;

	private final HdfsCopyStats stats;
	private final byte[] buffer;
	private final HdfsFetcher fetcher;
	private final int bufferSize;
	private final AsyncOperationStatus status;

	private static final Logger logger = Logger.getLogger(BasicFetchStrategy.class);

	public BasicFetchStrategy(	HdfsFetcher fetcher,
								FileSystem fs,
								HdfsCopyStats stats,
								AsyncOperationStatus status,
								int bufferSize) {
		this.fs = fs;
		this.stats = stats;
		this.status = status;
		this.buffer = new byte[bufferSize];
		this.bufferSize = bufferSize;
		this.fetcher = fetcher;
	}

	@Override
	public Map<HdfsFile, byte[]> fetch(HdfsDirectory directory, File dest)
			throws IOException {

		Map<HdfsFile, byte[]> fileCheckSumMap = new HashMap<HdfsFile, byte[]>(directory.getFiles()
																						.size());

		CheckSumType checkSumType = directory.getCheckSumType();
		for (HdfsFile file : directory.getFiles()) {
			String fileName = file.getDiskFileName();
			File copyLocation = new File(dest, fileName);
			CheckSum fileCheckSumGenerator = copyFileWithCheckSum(	file,
																	copyLocation,
																	checkSumType);
			if (fileCheckSumGenerator != null) {
				fileCheckSumMap.put(file, fileCheckSumGenerator.getCheckSum());
			}
		}
		return fileCheckSumMap;
	}

	/**
	 * Function to copy a file from the given filesystem with a checksum of type
	 * 'checkSumType' computed and returned. In case an error occurs during such
	 * a copy, we do a retry for a maximum of NUM_RETRIES
	 * 
	 * @param fs
	 *            Filesystem used to copy the file
	 * @param source
	 *            Source path of the file to copy
	 * @param dest
	 *            Destination path of the file on the local machine
	 * @param stats
	 *            Stats for measuring the transfer progress
	 * @param checkSumType
	 *            Type of the Checksum to be computed for this file
	 * @return A Checksum (generator) of type checkSumType which contains the
	 *         computed checksum of the copied file
	 * @throws IOException
	 */
	private CheckSum copyFileWithCheckSum(	HdfsFile source,
											File dest,
											CheckSumType checkSumType)
			throws IOException {
		CheckSum fileCheckSumGenerator = null;
		logger.debug("Starting copy of " + source + " to " + dest);

		// Check if its Gzip compressed
		boolean isCompressed = source.isCompressed();
		FilterInputStream input = null;

		OutputStream output = null;
		long startTimeMS = System.currentTimeMillis();

		for (int attempt = 0; attempt < fetcher.getMaxAttempts(); attempt++) {
			boolean success = true;
			long totalBytesRead = 0;
			boolean fsOpened = false;
			try {

				// Create a per file checksum generator
				if (checkSumType != null) {
					fileCheckSumGenerator = CheckSum.getInstance(checkSumType);
				}

				logger.info("Attempt " + attempt + " at copy of " + source
						+ " to " + dest);

				if (!isCompressed) {
					input = fs.open(source.getPath());
				} else {
					// We are already bounded by the "hdfs.fetcher.buffer.size"
					// specified in the Voldemort config, the default value of
					// which is 64K. Using the same as the buffer size for
					// GZIPInputStream as well.
					input = new GZIPInputStream(fs.open(source.getPath()),
												this.bufferSize);
				}
				fsOpened = true;

				output = new BufferedOutputStream(new FileOutputStream(dest));

				while (true) {
					int read = input.read(buffer);
					if (read < 0) {
						break;
					} else {
						output.write(buffer, 0, read);
					}

					// Update the per file checksum
					if (fileCheckSumGenerator != null) {
						fileCheckSumGenerator.update(buffer, 0, read);
					}

					// Check if we need to throttle the fetch
					if (fetcher.getThrottler() != null) {
						fetcher.getThrottler().maybeThrottle(read);
					}

					stats.recordBytes(read);
					totalBytesRead += read;
					if (stats.getBytesSinceLastReport() > fetcher.getReportingIntervalBytes()) {
						NumberFormat format = NumberFormat.getNumberInstance();
						format.setMaximumFractionDigits(2);
						logger.info(stats.getTotalBytesCopied()
								/ (1024 * 1024)
								+ " MB copied at "
								+ format.format(stats.getBytesPerSecond()
										/ (1024 * 1024)) + " MB/sec - "
								+ format.format(stats.getPercentCopied())
								+ " % complete, destination:" + dest);
						if (this.status != null) {
							this.status.setStatus(stats.getTotalBytesCopied()
									/ (1024 * 1024)
									+ " MB copied at "
									+ format.format(stats.getBytesPerSecond()
											/ (1024 * 1024)) + " MB/sec - "
									+ format.format(stats.getPercentCopied())
									+ " % complete, destination:" + dest);
						}
						stats.reset();
					}
				}
				stats.reportFileDownloaded(	dest,
											startTimeMS,
											source.getSize(),
											System.currentTimeMillis()
													- startTimeMS,
											attempt,
											totalBytesRead);
				logger.info("Completed copy of " + source + " to " + dest);

			} catch (IOException te) {
				success = false;
				if (!fsOpened) {
					logger.error("Error while opening the file stream to "
							+ source, te);
				} else {
					logger.error("Error while copying file " + source
							+ " after " + totalBytesRead + " bytes.", te);
				}
				if (te.getCause() != null) {
					logger.error("Cause of error ", te.getCause());
				}

				if (attempt < fetcher.getMaxAttempts() - 1) {
					logger.info("Will retry copying after "
							+ fetcher.getRetryDelayMs() + " ms");
					sleepForRetryDelayMs();
				} else {
					stats.reportFileError(	dest,
											fetcher.getMaxAttempts(),
											startTimeMS,
											te);
					logger.info("Fetcher giving up copy after "
							+ fetcher.getMaxAttempts() + " attempts");
					throw te;
				}
			} finally {
				IOUtils.closeQuietly(output);
				IOUtils.closeQuietly(input);
				if (success) {
					break;
				}
			}
			logger.debug("Completed copy of " + source + " to " + dest);
		}
		return fileCheckSumGenerator;
	}

	private void sleepForRetryDelayMs() {
		if (fetcher.getRetryDelayMs() > 0) {
			try {
				Thread.sleep(fetcher.getRetryDelayMs());
			} catch (InterruptedException ie) {
				logger.error("Fetcher interrupted while waiting to retry", ie);
			}
		}
	}

	@Override
	public CheckSum fetch(HdfsFile file, File dest, CheckSumType checkSumType)
			throws IOException {
		return copyFileWithCheckSum(file, dest, checkSumType);
	}

}
