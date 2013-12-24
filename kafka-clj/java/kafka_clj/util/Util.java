package kafka_clj.util;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;
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
	
	public static final ByteBuf writeUnsignedInt(ByteBuf buff, long v){
		return buff.writeInt((int)(v & 0xffffffffL));
	}	

}
