package snowflake.core.storage;

import snowflake.core.ChunkData;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2015.11.27_0
 * @author Johannes B. Latzel
 */
public interface IAllocateSpace {
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	ChunkData allocateSpace(long number_of_bytes);
	
	
}
