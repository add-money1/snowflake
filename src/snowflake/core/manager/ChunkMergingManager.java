package snowflake.core.manager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import com.sun.swing.internal.plaf.synth.resources.synth;

import j3l.util.Checker;
import j3l.util.LoopedTaskThread;
import snowflake.GlobalString;
import snowflake.StaticMode;
import snowflake.api.StorageException;
import snowflake.core.Chunk;
import snowflake.core.IChunk;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.07.11_0
 * @author Johannes B. Latzel
 */
public final class ChunkMergingManager {
	
	
	/**
	 * <p></p>
	 */
	private final ArrayList<Chunk> chunk_list;
	
	
	/**
	 * <p></p>
	 */
	private final IChunkManager chunk_manager;
	
	
	/**
	 * <p></p>
	 */
	private final LoopedTaskThread chunk_merging_thread;
	
	
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
	public ChunkMergingManager(IChunkManager chunk_manager) {
		if( StaticMode.TESTING_MODE ) {
			this.chunk_manager = Checker.checkForNull(chunk_manager, GlobalString.ChunkManager.toString());
		}
		else {
			this.chunk_manager = chunk_manager;
		}
		chunk_list = new ArrayList<>();
		chunk_merging_thread = new LoopedTaskThread(() -> {
			merge();
		}, "Snowflake ChunkMergingThread", 31_000);
		chunk_merging_thread.setPriority(Thread.MIN_PRIORITY);
		is_stopped = false;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public synchronized void start() {
		chunk_merging_thread.start();
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void stop() {
		is_stopped = true;
		chunk_merging_thread.interrupt();
	}
	
	
	/**
	 * <p>collects all chunks available and merges as much chunks as possible</p>
	 */
	private void merge() {
		Chunk[] chunk_array;
		synchronized( chunk_list ) {
			long chunk_list_size = chunk_list.size();
			if( chunk_list_size < 2 ) {
				return;
			}
			chunk_array = chunk_list.toArray(new Chunk[0]);
			chunk_list.clear();
		}
		Arrays.sort(chunk_array, (l,r) -> Long.compare(l.getStartAddress(), r.getStartAddress()));
		ArrayList<Chunk> processed_chunk_list = new ArrayList<>();
		ArrayList<Chunk> neighbour_list = new ArrayList<>();
		int current_index = 0; 
		int next_index = 1;
		int difference;
		do {
			if( is_stopped ) {
				for(int a=current_index;a<chunk_array.length;a++) {
					processed_chunk_list.add(chunk_array[a]);
				}
				break;
			}
			while( next_index < chunk_array.length && chunk_array[next_index - 1].isNeighbourOf(chunk_array[next_index]) ) {
				next_index++;
			}
			difference = next_index - current_index;
			if( difference > 1 ) {
				neighbour_list.ensureCapacity(difference);
				for(int a=current_index;a<next_index;a++) {
					neighbour_list.add(chunk_array[a]);
				}
				try {
					processed_chunk_list.add(chunk_manager.mergeChunks(neighbour_list));
				}
				catch( StorageException e ) {
					e.printStackTrace();
					break;
				}
				neighbour_list.clear();
			}
			else {
				processed_chunk_list.add(chunk_array[current_index]);
			}
			current_index = next_index;
			next_index++;
		}
		while( current_index < chunk_array.length );
		addAll(processed_chunk_list);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public Chunk getChunk(long minimum_length) {
		synchronized( chunk_list ) {
			if( !chunk_list.isEmpty() ) {
				chunk_list.sort((l, r) -> Long.compare(l.getLength(), r.getLength()));
				if( chunk_list.get(0).getLength() >= minimum_length ) {
					return chunk_list.remove(0);
				}
			}
			return null;
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public Collection<Chunk> removeAll() {
		ArrayList<Chunk> list;
		synchronized( chunk_list ) {
			list = new ArrayList<>(chunk_list);
			chunk_list.clear();
		}
		return list;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public boolean isEmpty() {
		synchronized( chunk_list ) {
			return chunk_list.isEmpty();
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public boolean add(Chunk chunk) {
		if( chunk == null ) {
			return false;
		}
		synchronized( chunk_list ) {
			if( !chunk_list.contains(chunk) ) {
				return chunk_list.add(chunk);
			}
			return false;
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public boolean addAll(Collection<? extends Chunk> chunk_collection) {
		if( chunk_collection == null || chunk_collection.size() == 0 ) {
			return false;
		}
		synchronized( chunk_list ) {
			return chunk_list.addAll(chunk_collection);
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public ArrayList<IChunk> getChunks() {
		synchronized( chunk_list ) {
			return new ArrayList<>(chunk_list);
		}
	}
	
}
