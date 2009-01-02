package voldemort.utils;

import java.util.Date;

/**
 * Time implementation that just reads from the system clock
 * 
 * @author jay
 * 
 */
public class SystemTime implements Time {

    public static SystemTime INSTANCE = new SystemTime();

    public Date getCurrentDate() {
        return new Date();
    }

    public long getMilliseconds() {
        return System.currentTimeMillis();
    }

    public long getNanoseconds() {
        return System.nanoTime();
    }

    public int getSeconds() {
        return (int) (getMilliseconds() / MS_PER_SECOND);
    }

}
