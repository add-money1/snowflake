package snowflake.core.manager;

import java.util.stream.LongStream;

import j3l.util.close.IStateClosure;
import snowflake.core.Chunk;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2015.12.07_0
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
	void openFlakes(LongStream flake_identification_stream);
	
	
}
