package snowflake.api.chunk;

import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Stream;

import j3l.util.check.ArgumentChecker;
import j3l.util.close.IStateClosure;
import j3l.util.stream.StreamFactory;
import j3l.util.stream.StreamMode;
import snowflake.core.Chunk;
import snowflake.core.flake.Flake;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2015.12.07_0
 * @author Johannes B. Latzel
 */
public interface IChunkManager extends IStateClosure  {
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	void appendChunk(Flake flake, long length, ChunkAppendingMode chunk_appending_mode);
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	default void appendChunk(Flake flake, long length) {
		appendChunk(flake, length, ChunkAppendingMode.MULTIPLE_CHUNKS);
	}
	
	
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
	default void recycleChunks(Chunk[] chunks) {
		recycleChunks(chunks, StreamMode.Parallel);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	default void recycleChunks(Chunk[] chunks, StreamMode stream_mode) {
		if( chunks != null && chunks.length > 0 ) {
			if( chunks.length == 1 ) {
				recycleChunk(chunks[0]);
			}
			else {
				recycleChunks(StreamFactory.getStream(chunks, stream_mode));
			}
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	default void recycleChunks(Collection<Chunk> chunk_collection) {
		recycleChunks(chunk_collection, StreamMode.Parallel);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	default void recycleChunks(Collection<Chunk> chunk_collection, StreamMode stream_mode) {
		if( chunk_collection != null && !chunk_collection.isEmpty() ) {
			recycleChunks(StreamFactory.getStream(new ArrayList<>(chunk_collection), stream_mode));
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	default void recycleChunks(Stream<Chunk> chunk_stream) {
		ArgumentChecker.checkForNull(chunk_stream, "chunk_stream")
		.filter(chunk -> chunk != null && chunk.isValid()).forEach(chunk -> recycleChunk(chunk));
	}
	
	
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
