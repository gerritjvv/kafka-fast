package kafka_clj.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import java.util.zip.GZIPInputStream;

import org.iq80.snappy.SnappyInputStream;

import io.netty.buffer.ByteBuf;

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

	public static final byte[] deflateSnappy(byte[] bts) throws Exception{
	  SnappyInputStream in = new SnappyInputStream(new ByteArrayInputStream(bts));
	  ByteArrayOutputStream out = new ByteArrayOutputStream();
	  
	  int i = -1;
		
	  while( (i = in.read()) != -1)
			out.write(i);
		
	  return out.toByteArray();
	}
	
	public static final byte[] deflateGzip(byte[] bts) throws IOException{
		GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(bts));
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		int i = -1;
		
		while( (i = in.read()) != -1)
			out.write(i);
		
		return out.toByteArray();
	}
	
}
