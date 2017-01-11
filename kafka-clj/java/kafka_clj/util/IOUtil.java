package kafka_clj.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.TimeoutException;

/**
 * Mutable IO Utility functions used in clojure code.
 */
public class IOUtil {

    public static final void writeInt(OutputStream out, int v) throws IOException {
        out.write((v >>> 24) & 0xFF);
        out.write((v >>> 16) & 0xFF);
        out.write((v >>>  8) & 0xFF);
        out.write((v >>>  0) & 0xFF);
    }

    /**
     * Read 4 bytes and return an int
     * @param in
     * @param timeoutMs
     * @return
     * @throws InterruptedException
     * @throws IOException
     * @throws TimeoutException
     */
    public static final int readInt(InputStream in, long timeoutMs) throws InterruptedException, IOException, TimeoutException {
        byte[] bts = readBytes(in, 4, timeoutMs);
        return (((bts[0] & 0xff) << 24) | ((bts[1] & 0xff) << 16) |
                ((bts[2] & 0xff) << 8) | (bts[3] & 0xff));
    }

    /**
     * Read expectedBytes from an input stream and time out if no change on the input stream for more than timeoutMs.<br/>
     * Bytes are read from the input stream as they become available.
     * @param in
     * @param expectedBytes the total number of bytes to read, a byte array of this size is created
     * @param timeoutMs
     * @throws TimeoutException
     * @throws IOException
     * @return
     */
    public static final byte[] readBytes(InputStream in, int expectedBytes, long timeoutMs) throws TimeoutException, IOException, InterruptedException {
        byte[] bytes = new byte[expectedBytes];
        int pos = 0;
        long lastTimeAvailable = System.currentTimeMillis();

        do{
            int avail = in.available();
            if(avail > 0){
                //have an new data, read the available bytes and add to the byte array, note we use expectedBytes-pos, to calculate the remaining bytes
                //required to read
                int btsRead = in.read(bytes, pos, Math.min(avail, expectedBytes - pos));
                pos += btsRead;

                if(pos >= expectedBytes) //we've read all required bytes, exit the loop
                    break;

                //save the last time data was available
                lastTimeAvailable = System.currentTimeMillis();
            }else if((System.currentTimeMillis() - lastTimeAvailable) > timeoutMs){
               //check for timeout
               throw new TimeoutException("Timeout while reading data from the input stream: got only " + pos + " bytes of " + expectedBytes + " last seen " + lastTimeAvailable + " diff " + (System.currentTimeMillis() - lastTimeAvailable));
            }else{
                //sleep to simulate IO blocking and avoid consuming CPU resources on IO wait
                Thread.sleep(100);
            }

        } while(true);

        return bytes;
    }
}
