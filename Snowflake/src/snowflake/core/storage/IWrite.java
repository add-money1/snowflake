package snowflake.core.storage;

import java.io.IOException;

import j3l.util.check.ArgumentChecker;
import snowflake.api.flake.DataPointer;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2015.12.04_0
 * @author Johannes B. Latzel
 */
public interface IWrite {

	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	void write(DataPointer data_pointer, byte b) throws IOException;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	default void write(DataPointer data_pointer, byte[] buffer) throws IOException {

		ArgumentChecker.checkForNull(buffer, "buffer");
		
		if( buffer.length == 0 ) {
			return;
		}
		
		write(data_pointer, buffer, 0, buffer.length);
		
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	void write(DataPointer data_pointer, byte[] buffer, int offset, int length) throws IOException;
	
}
