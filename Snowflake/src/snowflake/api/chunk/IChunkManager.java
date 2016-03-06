package snowflake.api.chunk;

import java.util.Collection;

import j3l.util.close.IStateClosure;
import snowflake.core.data.Chunk;
import snowflake.core.flake.Flake;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.03.06_0
 * @author Johannes B. Latzel
 */
public interface IChunkManager extends IStateClosure  {
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	void appendChunk(Flake flake, long length);
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	void recycleChunk(Chunk chunk);
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	void recycleChunks(Collection<Chunk> chunk_collection);
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	Chunk mergeChunks(Chunk[] chunks);
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	Chunk mergeChunks(Collection<Chunk> chunk_collection);
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	Chunk[] splitChunk(Chunk chunk, long position);
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	Chunk trimToSize(Chunk chunk, long size);
	
}
