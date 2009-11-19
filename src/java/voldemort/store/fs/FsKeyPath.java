package voldemort.store.fs;

import java.io.File;

import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;

/**
 * Encapsulation of the key information used by the fs store including the file
 * path this key maps to
 * 
 * @author jay
 * 
 */
final public class FsKeyPath {

    private final static String SLASH = System.getProperty("file.separator");
    private final static char[] HEX_DIGITS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            'a', 'b', 'c', 'd', 'e', 'f' };

    private final ByteArray key;
    private final byte[] digest;
    private final String path;
    private final int id;

    FsKeyPath(ByteArray key, byte[] digest, String path, int id) {
        this.key = key;
        this.digest = digest;
        this.path = path;
        this.id = id;
    }

    public static FsKeyPath forKey(ByteArray key,
                                   File[] baseDirs,
                                   int depth,
                                   int fanOut,
                                   int dirNameLength) {
        byte[] digest = ByteUtils.sha1(key.get());
        int id = Math.abs(ByteUtils.readInt(digest, 0));
        String baseDir = baseDirs[id % baseDirs.length].getAbsolutePath();
        StringBuilder path = new StringBuilder(baseDir);
        path.append(SLASH);

        // hash to directory
        for(int i = 0; i < depth; i++) {
            String dirId = Integer.toString(Math.abs(ByteUtils.readInt(digest, 4 * i)) % fanOut);
            int padding = dirNameLength - dirId.length();
            for(int j = 0; j < padding; j++)
                path.append('0');
            path.append(dirId);
            path.append(SLASH);
        }

        // add file name as hex string
        for(int i = 0; i < digest.length; i++)
            path.append(HEX_DIGITS[Math.abs(digest[i] % HEX_DIGITS.length)]);

        return new FsKeyPath(key, digest, path.toString(), id);
    }

    /**
     * The key that was hashed
     */
    public ByteArray getKey() {
        return key;
    }

    /**
     * The digest of the key
     */
    public byte[] getDigest() {
        return digest;
    }

    /**
     * The absolute path to the key/value pair
     */
    public String getPath() {
        return path;
    }

    public int getId() {
        return id;
    }

}
