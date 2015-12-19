package snowflake.core.storage;

import snowflake.core.data.Chunk;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2015.11.29_0
 * @author Johannes B. Latzel
 */
public interface IClearChunk {
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	void clearChunk(Chunk chunk);
	
}
