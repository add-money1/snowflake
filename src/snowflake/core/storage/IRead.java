package snowflake.core.storage;

import java.nio.ByteBuffer;

import snowflake.api.DataPointer;
import snowflake.core.Returnable;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.08.12_0
 * @author Johannes B. Latzel
 */
public interface IRead extends Returnable {
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	int read(DataPointer data_pointer, ByteBuffer buffer);
	
}
