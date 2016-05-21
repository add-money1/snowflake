package snowflake.core.manager;

import java.util.Collection;

import j3l.util.close.IStateClosure;
import snowflake.core.Chunk;
import snowflake.core.Flake;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.05.03_0
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
