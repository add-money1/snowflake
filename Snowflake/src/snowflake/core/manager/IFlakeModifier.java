package snowflake.core.manager;

import j3l.util.close.IStateClosure;
import snowflake.core.data.Chunk;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.03.10_0
 * @author Johannes B. Latzel
 */
public interface IFlakeModifier extends IStateClosure {
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	void addChunkToFlake(long identification, Chunk chunk, int index, ChunkManager chunk_manager);
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	void openFlakes();
	
	
}
