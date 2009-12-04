import java.io.*;

import junit.framework.TestCase;
import static org.junit.Assert.*;

import org.h2.compress.*;

public class TestLZF extends TestCase
{
    /**
     * Semi-automatic unit test: will use all files on current working
     * directory (and its subdirs) for testing that LZF encode+decode
     * will correctly round-trip content.
     */
    public void testWithFiles() throws Exception
    {
        File currDir = new File("").getAbsoluteFile();
        int count = _handleFiles(currDir);
        System.out.println("OK: tested with "+count+" files.");
    }

    private int _handleFiles(File dir) throws IOException
    {
        System.out.println("Testing files from dir '"+dir.getAbsolutePath()+"'...");
        int count = 0;
        for (File f : dir.listFiles()) {
            if (f.isDirectory()) {
                count += _handleFiles(f);
            } else {
                byte[] data = _readData(f);
                byte[] enc = LZFEncoder.encode(data);
                byte[] dec = LZFDecoder.decode(enc);
                assertArrayEquals("File '"+f.getAbsolutePath()+"'", data, dec);
                ++count;
            }
        }
        return count;
    }

    private static byte[] _readData(File in) throws IOException
    {
        int len = (int) in.length();
        byte[] result = new byte[len];
        int offset = 0;
        FileInputStream fis = new FileInputStream(in);

        while (len > 0) {
            int count = fis.read(result, offset, len);
            if (count < 0) break;
            len -= count;
            offset += count;
        }
        fis.close();
        return result;
    }
    
}