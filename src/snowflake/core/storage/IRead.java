package snowflake.core.storage;

import java.io.IOException;

import j3l.util.Checker;
import snowflake.GlobalString;
import snowflake.api.DataPointer;
import snowflake.core.Returnable;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.04.07_0
 * @author Johannes B. Latzel
 */
public interface IRead extends Returnable {
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	byte read(DataPointer data_pointer) throws IOException;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	default int read(DataPointer data_pointer, byte[] buffer) throws IOException {
		if( Checker.checkForNull(buffer, GlobalString.Buffer.toString()).length == 0 ) {
			return 0;
		}
		return read(data_pointer, buffer, 0, buffer.length);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	int read(DataPointer data_pointer, byte[] buffer, int offset, int length) throws IOException;
	
}
