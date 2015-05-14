package voldemort.store.readonly.fetcher;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import voldemort.VoldemortApplicationException;
import voldemort.store.readonly.ReadOnlyStorageMetadata;
import voldemort.store.readonly.checksum.CheckSum;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;
import voldemort.store.readonly.fetcher.HdfsFile.FileType;
import voldemort.utils.ByteUtils;


public class HdfsDirectory {

    private static final Logger logger = Logger.getLogger(HdfsDirectory.class);

    private final FileSystem fs;
    private final Path source;
    private CheckSumType checkSumType = CheckSumType.NONE;
    private byte[] expectedCheckSum = null;
    private List<HdfsFile> allFiles = new ArrayList<HdfsFile>();
    private HdfsFile metadataFile = null;
    private ReadOnlyStorageMetadata metadata;

    private static final String CHECKSUM_FILE = "checkSum.txt";

    private void setMetadataFile(HdfsFile file) {
        if(metadataFile != null) {
            throw new VoldemortApplicationException("more than one metadata file present existing file "
                                                    + metadataFile.getPath()
                                                    + " new file "
                                                    + file.getPath());
        }
        metadataFile = file;
    }

    public HdfsFile getMetadataFile() {
        return metadataFile;
    }

    public HdfsDirectory(FileSystem fs, Path source) throws IOException {
        this.fs = fs;
        this.source = source;

        FileStatus[] files = fs.listStatus(source);
        if(files == null) {
            throw new VoldemortApplicationException(source + " is empty");
        }

        for(FileStatus file: files) {
            String fileName = file.getPath().getName();
            if(fileName.contains(CHECKSUM_FILE)
               || (!fileName.contains(HdfsFetcher.METADATA_FILE_EXTENSION) && fileName.startsWith("."))) {
                continue;
            }
            
            HdfsFile hdfsFile = new HdfsFile(file);
            if(hdfsFile.getFileType() == FileType.METADATA) {
                setMetadataFile(hdfsFile);
            } else {
                allFiles.add(hdfsFile);
            }
        }

        Collections.sort(allFiles);
    }

    public void initializeMetadata(File diskFile) {
        try {
            metadata = new ReadOnlyStorageMetadata(diskFile);
        } catch(IOException e) {
            logger.error("Error reading metadata file ", e);
            throw new VoldemortApplicationException(e);
        }

        checkSumType = metadata.getCheckSumType();
        if(checkSumType != CheckSumType.NONE) {
            try {
            expectedCheckSum = metadata.getCheckSum();
            } catch(DecoderException e) {
                logger.error("Error decoding checksum", e);
                throw new VoldemortApplicationException(e);
            }
        }
    }

    public List<HdfsFile> getFiles() {
        return allFiles;
    }

    public CheckSumType getCheckSumType() {
        return checkSumType;
    }

    public boolean validateCheckSum(Map<HdfsFile, byte[]> fileCheckSumMap) {
        if(checkSumType == CheckSumType.NONE) {
            logger.info("No check-sum verification required");
            return true;
        }

        CheckSum checkSumGenerator = CheckSum.getInstance(checkSumType);

        for(HdfsFile file: allFiles) {
            byte[] fileCheckSum = fileCheckSumMap.get(file);
            if(fileCheckSum != null) {
                checkSumGenerator.update(fileCheckSum);
            }
        }

        byte[] computedCheckSum = checkSumGenerator.getCheckSum();

        boolean checkSumComparison = (ByteUtils.compare(expectedCheckSum, computedCheckSum) == 0);

        logger.info("Checksum generated from streaming - "
                    + new String(Hex.encodeHex(computedCheckSum)));
        logger.info("Checksum on file - " + new String(Hex.encodeHex(expectedCheckSum)));
        logger.info("Check-sum verification - " + checkSumComparison);
        return checkSumComparison;
    }

}
