package snowflake.core.manager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import j3l.util.LoopedTaskThread;
import j3l.util.check.ArgumentChecker;
import snowflake.GlobalString;
import snowflake.StaticMode;
import snowflake.core.Chunk;
import snowflake.core.storage.IClearChunk;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.07.02_0
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
	private final ArrayList<Chunk> available_chunk_list;
	
	
	/**
	 * <p></p>
	 */
	private final IClearChunk clear_chunk;
	
	
	/**
	 * <p></p>
	 */
	private final long chunk_recycling_threshhold;
	
	
	/**
	 * <p></p>
	 */
	private final LoopedTaskThread chunk_recycling_thread;
	
	
	/**
	 * <p></p>
	 */
	private boolean is_stopped;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public ChunkRecyclingManager(IClearChunk clear_chunk, long chunk_recycling_threshhold) {
		if( StaticMode.TESTING_MODE ) {
			this.clear_chunk = ArgumentChecker.checkForNull(clear_chunk, GlobalString.ClearChunk.toString());
			this.chunk_recycling_threshhold = ArgumentChecker.checkForBoundaries(
				chunk_recycling_threshhold, 1, Long.MAX_VALUE, GlobalString.CleaningTreshhold.toString()
			);
		}
		else {
			this.clear_chunk = clear_chunk;
			this.chunk_recycling_threshhold = chunk_recycling_threshhold;
		}
		chunk_recycling_list = new ArrayList<>(1000);
		available_chunk_list = new ArrayList<>(1000);
		chunk_recycling_thread = new LoopedTaskThread(this::recycle, "Snowflake ChunkRecyclingThread", 1000);
		chunk_recycling_thread.setPriority(Thread.MIN_PRIORITY);
		is_stopped = false;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public synchronized void start() {
		chunk_recycling_thread.start();
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void stop() {
		is_stopped = true;
		chunk_recycling_thread.interrupt();
	}
	
	
	/**
	 * <p>recycles one of the chunks in {@link #chunk_recycling_tree} and puts it into {@link #available_chunk_list}</p>
	 */
	private void recycle() {
		long number_of_remaining_bytes = chunk_recycling_threshhold;
		Chunk current_chunk;
		do {
			if( is_stopped ) {
				return;
			}
			synchronized( chunk_recycling_list ) {
				if( chunk_recycling_list.isEmpty() ) {
					return;
				}
				current_chunk = chunk_recycling_list.remove(chunk_recycling_list.size() - 1);
			}
			clear_chunk.clearChunk(current_chunk);
			number_of_remaining_bytes -= current_chunk.getLength();
			current_chunk.setNeedsToBeCleared(false);
			current_chunk.save(null);
			synchronized( available_chunk_list ) {
				available_chunk_list.add(current_chunk);
			}
		}
		while( number_of_remaining_bytes > 0 );
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public boolean isEmpty() {
		synchronized( chunk_recycling_list ) {
			synchronized( available_chunk_list ) {
				return chunk_recycling_list.isEmpty() && available_chunk_list.isEmpty();
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
		if( StaticMode.TESTING_MODE ) {
			ArgumentChecker.checkForValidation(chunk, GlobalString.Chunk.toString());
		}
		chunk.setNeedsToBeCleared(true);
		chunk.resetPositionInFlake();
		chunk.save(null);
		synchronized( chunk_recycling_list ) {
			if( !chunk_recycling_list.contains(chunk) ) {
				return chunk_recycling_list.add(chunk);
			}
		}
		throw new SecurityException("No chunk must ever be managed twice at the same time!");
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public boolean addAll(Collection<Chunk> chunk_collection) {
		if( StaticMode.TESTING_MODE ) {
			ArgumentChecker.checkForNull(chunk_collection, GlobalString.ChunkCollection.toString());
		}
		if( chunk_collection.size() > 0 ) {
			for( Chunk chunk : chunk_collection ) {
				if( StaticMode.TESTING_MODE ) {
					ArgumentChecker.checkForValidation(chunk, GlobalString.Chunk.toString());
				}
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
		synchronized( available_chunk_list ) {
			list = new ArrayList<>(available_chunk_list);
			available_chunk_list.clear();
		}
		return list;
	}	
	
}
