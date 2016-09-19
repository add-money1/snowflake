package snowflake;

import java.io.IOException;
import java.nio.ByteBuffer;

import snowflake.core.FlakeInputStream;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.09.19_0
 * @author Johannes B. Latzel
 */
public final class Util {
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 * @throws IOException 
	 */
	public static ByteBuffer readComplete(FlakeInputStream input_stream, ByteBuffer buffer) throws IOException {
		int current_read_in_bytes;
		while( buffer.remaining() > 0 ) {
			current_read_in_bytes = input_stream.read(buffer);
			if( current_read_in_bytes < 0 ) {
				break;
			}
		}
		buffer.flip();
		return buffer;
	}
	
	
}
