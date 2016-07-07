package snowflake.core.manager;

import java.util.Collection;

import j3l.util.close.IStateClosure;
import snowflake.core.Chunk;
import snowflake.core.SplitChunk;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.07.02_0
 * @author Johannes B. Latzel
 */
public interface IChunkManager extends IStateClosure  {
	
	
	/**
	 * <p>collection of chunks with length-sum of number_of_bytes</p>
	 * <p>the array is guaranteed to not include nulls</p>
	 *
	 * @param number_of_bytes number of bytes
	 * @return Collection of chunks
	 */
	Collection<Chunk> allocateSpace(long number_of_bytes);
	
	
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
	SplitChunk splitChunk(Chunk chunk, long position);
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	Chunk trimToSize(Chunk chunk, long size);
	
}
