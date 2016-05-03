package snowflake.core.manager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import j3l.util.check.ArgumentChecker;
import snowflake.api.GlobalString;
import snowflake.api.StorageException;
import snowflake.core.Chunk;
import snowflake.core.storage.IClearChunk;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.03.10_0
 * @author Johannes B. Latzel
 */
public final class ChunkRecyclingManager {
	
	
	/**
	 * <p></p>
	 */
	private final ArrayList<Chunk> chunk_recycling_list;
	
	
	/**
	 * <p></p>
	 */
	private final ArrayList<Chunk> recycled_chunk_list;
	
	
	/**
	 * <p></p>
	 */
	private final IClearChunk clear_chunk;
	
	
	/**
	 * <p></p>
	 */
	private final long cleaning_treshhold;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public ChunkRecyclingManager(IClearChunk clear_chunk, long cleaning_treshhold) {
		this.clear_chunk = ArgumentChecker.checkForNull(clear_chunk, GlobalString.ClearChunk.toString());
		this.cleaning_treshhold = ArgumentChecker.checkForBoundaries(
			cleaning_treshhold, 1, Long.MAX_VALUE, GlobalString.CleaningTreshhold.toString()
		);
		chunk_recycling_list = new ArrayList<>(1000);
		recycled_chunk_list = new ArrayList<>(1000);
	}
	
	
	/**
	 * <p>recycles one of the chunks in {@link #chunk_recycling_tree} and puts it into {@link #recycled_chunk_list}</p>
	 */
	public void recycleChunk() {
		
		Chunk chunk = null;
		
		synchronized( chunk_recycling_list ) {
			if( !chunk_recycling_list.isEmpty() ) {
				chunk = chunk_recycling_list.remove(chunk_recycling_list.size() - 1);
			}
		}
		
		if( chunk == null ) {
			return;
		}
		
		long remaining_bytes = chunk.getLength();
		long advance;
		while( remaining_bytes > 0 ) {
			advance = cleaning_treshhold < remaining_bytes ? cleaning_treshhold : remaining_bytes;
			try {
				clear_chunk.clearChunk(chunk, chunk.getLength() - remaining_bytes, advance);
			}
			catch( StorageException e ) {
				e.printStackTrace();
				synchronized( chunk_recycling_list ) {
					chunk_recycling_list.add(chunk);
				}
				return;
			}
			remaining_bytes -= advance;
		}
		
		chunk.setNeedsToBeCleared(false);
		chunk.save(null);
		
		synchronized( recycled_chunk_list ) {
			recycled_chunk_list.add(chunk);
		}
		
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public boolean isEmpty() {
		synchronized( chunk_recycling_list ) {
			synchronized( recycled_chunk_list ) {
				return chunk_recycling_list.isEmpty() && recycled_chunk_list.isEmpty();
			}
		}
	}
	
	
	/**
	 * <p>adds a chunk to this manager</p>
	 *
	 * @param chunk the chunk
	 * @return true if the chunk has been added, false otherwise
	 */
	public boolean add(Chunk chunk) {
		ArgumentChecker.checkForValidation(chunk, GlobalString.Chunk.toString());
		chunk.setNeedsToBeCleared(true);
		chunk.resetPositionInFlake();
		chunk.save(null);
		synchronized( chunk_recycling_list ) {
			if( !chunk_recycling_list.contains(chunk) ) {
				return chunk_recycling_list.add(chunk);
			}
			else {
				throw new SecurityException("No chunk must ever be managed twice at the same time!");
			}
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public boolean addAll(Collection<Chunk> chunk_collection) {
		if( ArgumentChecker.checkForNull(chunk_collection, GlobalString.ChunkCollection.toString()).size() > 0 ) {
			for( Chunk chunk : chunk_collection ) {
				ArgumentChecker.checkForValidation(chunk, GlobalString.Chunk.toString());
				chunk.setNeedsToBeCleared(true);
				chunk.resetPositionInFlake();
				chunk.save(null);
			}
			synchronized( chunk_recycling_list ) {
				return chunk_recycling_list.addAll(chunk_collection);
			}
		}
		return false;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public List<Chunk> removeAll() {
		ArrayList<Chunk> list;
		synchronized( recycled_chunk_list ) {
			list = new ArrayList<>(recycled_chunk_list);
			recycled_chunk_list.clear();
		}
		return list;
	}	
	
}
