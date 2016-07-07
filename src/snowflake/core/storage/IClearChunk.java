package snowflake.core.storage;

import snowflake.api.StorageException;
import snowflake.core.Chunk;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.01.20_0
 * @author Johannes B. Latzel
 */
public interface IClearChunk {
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	default void clearChunk(Chunk chunk) throws StorageException {
		if( chunk != null && !chunk.isValid() ) {
			clearChunk(chunk, 0, chunk.getLength());
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	void clearChunk(Chunk chunk, long offset, long length) throws StorageException;
	
}
