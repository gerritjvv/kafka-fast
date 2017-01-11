package kafka_clj.util;

import kafka.utils.Time;

class SystemTime implements Time {
    public long milliseconds() {
        return System.currentTimeMillis();
    }

    public long nanoseconds() {
        return System.nanoTime();
    }

    @Override
    public long hiResClockMs() {
        return 0;
    }

    public void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            // Ignore
        }
    }
}