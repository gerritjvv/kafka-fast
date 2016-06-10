package kafka_clj.util;

import com.alexkasko.unsafe.offheap.OffHeapMemory;
import io.netty.buffer.ByteBuf;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import org.apache.commons.io.IOUtils;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyInputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import java.util.zip.GZIPInputStream;


public class Util {

    /**
     * Calculates the crc32 and casts it to an integer,
     * this avoids clojure's number autoboxing
     *
     * @param bts
     * @return
     */
    public static final long crc32(byte[] bts) {
        final CRC32 crc = new CRC32();
        crc.update(bts);
        return crc.getValue();
    }

    public static final ByteBuf setUnsignedInt(ByteBuf buff, int pos, long v) {
        return buff.setInt(pos, (int) (v & 0xffffffffL));
    }


    public static final short readShort(OffHeapMemory memory, long pos) {
        return (short) ((memory.getByte(pos++) << 8) | (memory.getByte(pos++) & 0xff));
    }

    public static final int readInt(OffHeapMemory memory, long pos) {
        return (((memory.getByte(pos++) & 0xff) << 24) | ((memory.getByte(pos++) & 0xff) << 16) |
                ((memory.getByte(pos++) & 0xff) << 8) | (memory.getByte(pos++) & 0xff));
    }

    public static final long readLong(OffHeapMemory memory, long pos) {
        return (((long) (memory.getByte(pos++) & 0xff) << 56) |
                ((long) (memory.getByte(pos++) & 0xff) << 48) |
                ((long) (memory.getByte(pos++) & 0xff) << 40) |
                ((long) (memory.getByte(pos++) & 0xff) << 32) |
                ((long) (memory.getByte(pos++) & 0xff) << 24) |
                ((long) (memory.getByte(pos++) & 0xff) << 16) |
                ((long) (memory.getByte(pos++) & 0xff) << 8) |
                ((long) (memory.getByte(pos++) & 0xff)));
    }

    public static byte[] getBytes(OffHeapMemory memory, long pos, long len) {
        byte[] bts = new byte[(int) len];
        for (int i = 0; i < len; i++) {
            bts[i] = memory.getByte(pos++);
        }
        return bts;
    }


    public static final ByteBuf writeUnsignedInt(ByteBuf buff, long v) {
        return buff.writeInt((int) (v & 0xffffffffL));
    }

    public static final long unsighedToNumber(long v) {
        return v & 0xFFFFFFFFL;
    }


    public static final byte[] deflateLZ4(final byte[] bts) throws Exception {
        LZ4BlockInputStream lz4In = new LZ4BlockInputStream(new ByteArrayInputStream(bts));
        ByteArrayOutputStream btOut = new ByteArrayOutputStream();

        IOUtils.copy(lz4In, btOut);
        lz4In.close();
        btOut.close();
        return btOut.toByteArray();
    }

    public static final byte[] deflateSnappy(final byte[] bts) throws Exception {
        //do not change Snappy.unCompress nor any other method works.
        final int buffLen = 2 * bts.length;
        final SnappyInputStream in = new SnappyInputStream(new ByteArrayInputStream(bts));
        final ByteArrayOutputStream out = new ByteArrayOutputStream(buffLen);
        int len = 0;
        final byte[] buff = new byte[buffLen];

        try {
            while ((len = in.read(buff, 0, buff.length)) > 0)
                out.write(buff, 0, len);
        } finally {
            in.close();
        }

        return out.toByteArray();
    }

    public static final byte[] compressLZ4(byte[] bts) throws Exception {
        ByteArrayOutputStream btarr = new ByteArrayOutputStream();
        LZ4BlockOutputStream lz4Out = new LZ4BlockOutputStream(btarr, 1024 * 64);
        lz4Out.write(bts);
        lz4Out.close();
        return btarr.toByteArray();
    }

    public static final byte[] compressSnappy(byte[] bts) throws Exception {
        return Snappy.compress(bts);
    }


    public static final byte[] deflateGzip(final byte[] bts) throws IOException {
        final int buffLen = 2 * bts.length;
        final GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(bts));
        final ByteArrayOutputStream out = new ByteArrayOutputStream(buffLen);
        int len = 0;
        final byte[] buff = new byte[buffLen];

        try {

            while ((len = in.read(buff)) > 0)
                out.write(buff, 0, len);

        } finally {
            in.close();
        }
        return out.toByteArray();
    }

    public static String asStr(ByteBuffer buff){
        if(buff.hasArray())
            return new String(buff.array(), buff.arrayOffset()+buff.position(), buff.limit());
        else
            return new String(toBytes(buff));
    }

    public static String asStr(ByteBuf buff){
        if(buff.hasArray())
            return new String(buff.array(), buff.arrayOffset(), buff.readableBytes());
        else
            return new String(toBytes(buff));
    }

    public static final String[] strArray(String str) {
        return new String[]{str};
    }

    public static byte[] toBytes(ByteBuffer buff){
        int limit = buff.limit();
        byte[] arr = new byte[limit];
        System.arraycopy(buff.array(), buff.arrayOffset()+buff.position(), arr, 0, limit);
        return arr;
    }

    public static byte[] toBytes(ByteBuf buff){
        ByteBuf buff2 = buff.slice();
        byte[] bts = new byte[buff2.readableBytes()];

        buff2.readBytes(bts);

        return bts;
    }

    public final static boolean isNippyCompressed(byte[] bts){
        return (bts.length > 3
                && bts[0] == 78   //N
                && bts[1] == 80   //P
                && bts[2] == 89); //Y
    }

    public final static byte[] byteString(Object obj) throws UnsupportedEncodingException {
        return obj.toString().getBytes("UTF-8");
    }

    public static final String correctURI(String uri){
        String str = uriExternalForm(createURI(uri));
        return (str.startsWith("//")) ? str.substring(2, str.length()) : str;
    }

    public static final String uriExternalForm(URI uri){
        return parseCorrectURI(uri.toString());
    }

    public static final URI createURI(String uri) {
        return URI.create("//" + parseCorrectURI(uri));
    }

    private static final String parseCorrectURI(String uri){
        String[] parts = uri.split(":");

        if (parts.length-1 >= 3) {
            String port = parts[parts.length-1];
            uri = "[" + uri.replace(":" + port, "") + "]:" + port;
        }
        return uri;
    }

    public static final byte[] decompress(int codec, byte[] bts){
       try{
           switch (codec){
               case 0:
                   return bts;
               case 1:
                   return Util.deflateGzip(bts);
               case 2:
                   return Util.deflateSnappy(bts);
               case 3:
                   return Util.deflateLZ4(bts);
               default:
                   throw new RuntimeException("Codec " + codec + " is not supported");
           }
       }catch(Exception e){
           RuntimeException rte = new RuntimeException(e);
           rte.setStackTrace(e.getStackTrace());
           throw rte;
       }
    }
}
