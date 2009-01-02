package voldemort;

import java.util.Date;

import voldemort.utils.Time;

public class MockTime implements Time {
    
    private long currentTime;
    
    public MockTime() {
        this.currentTime = System.currentTimeMillis();
    }
    
    public MockTime(long time) {
        this.currentTime = time;
    }

    public Date getCurrentDate() {
        return new Date(currentTime);
    }

    public long getMilliseconds() {
        return this.currentTime;
    }

    public long getNanoseconds() {
        return this.currentTime;
    }

    public int getSeconds() {
        return (int) (getMilliseconds() / MS_PER_SECOND);
    }

    public void setTime(long time) {
        this.currentTime = time;
    }
    
    public void setCurrentDate(Date date) {
        this.currentTime = date.getTime();
    }
    
    public void addMilliseconds(long ms) {
        this.currentTime += ms;
    }
    
}
