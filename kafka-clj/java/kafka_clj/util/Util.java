package kafka_clj.util;

import io.netty.buffer.ByteBuf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.CRC32;
import java.util.zip.GZIPInputStream;

import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyInputStream;


public class Util {

	/**
	 * Calculates the crc32 and casts it to an integer,
	 * this avoids clojure's number autoboxing
	 * @param bts
	 * @return
	 */
	public static final long crc32(byte[] bts){
		final CRC32 crc = new CRC32();
		crc.update(bts);
		return crc.getValue();
	}

	public static final ByteBuf setUnsignedInt(ByteBuf buff, int pos, long v){
		return buff.setInt(pos, (int)(v & 0xffffffffL));
	}	

	public static final ByteBuf writeUnsignedInt(ByteBuf buff, long v){
		return buff.writeInt((int)(v & 0xffffffffL));
	}	
	
	public static final long unsighedToNumber(long v){
	   return v & 0xFFFFFFFFL;
    }

	public static final byte[] deflateSnappy(final byte[] bts) throws Exception{
		final int buffLen = 2 * bts.length;
		final SnappyInputStream in = new SnappyInputStream(new ByteArrayInputStream(bts));
		final ByteArrayOutputStream out = new ByteArrayOutputStream(buffLen);
		int len = 0;
		final byte[] buff = new byte[buffLen];
		
		try{
			while((len = in.read(buff, 0, buff.length)) > 0)
				out.write(buff, 0, len);
		}finally{
			in.close();
		}
		
		return out.toByteArray();
	}
	
	public static final byte[] compressSnappy(byte[] bts) throws Exception{
		return Snappy.compress(bts);
	}
	
	
	public static final byte[] deflateGzip(final byte[] bts) throws IOException{
		final int buffLen = 2 * bts.length;
		final GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(bts));
		final ByteArrayOutputStream out = new ByteArrayOutputStream(buffLen);
		int len = 0;
		final byte[] buff = new byte[buffLen];
		
		try{
			
			while((len = in.read(buff)) > 0)
				out.write(buff, 0, len);
			
		}finally{
			in.close();
		}
		return out.toByteArray();
	}
	
}
