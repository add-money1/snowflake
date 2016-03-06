package snowflake.core.flake;

import java.util.ArrayList;
import java.util.Collection;

import j3l.util.check.ArgumentChecker;
import j3l.util.stream.StreamFactory;
import j3l.util.stream.StreamMode;
import snowflake.api.chunk.IChunkInformation;
import snowflake.api.chunk.IChunkManager;
import snowflake.core.data.Chunk;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.06.03_0
 * @author Johannes B. Latzel
 */
public final class FlakeDataManager {
	
	
	/**
	 * <p></p>
	 */
	private final static int CHUNK_SEARCH_THRESHOLD = 50;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private static boolean areNeighboursInFlake(Chunk left, Chunk right) {
		return 	( left.getPositionInFlake() == (right.getPositionInFlake() + right.getLength()) ) 
				|| ( right.getPositionInFlake() == (left.getPositionInFlake() + left.getLength()) );
	}


	/**
	 * <p></p>
	 */
	private long length;
	
	
	/**
	 * <p></p>
	 */
	private boolean is_length_changed;
	
	
	/**
	 * <p></p>
	 */
	private final ArrayList<Chunk> chunk_list;
	
	
	/**
	 * <p></p>
	 */
	private final Flake flake;
	
	
	/**
	 * <p></p>
	 */
	private final Object chunk_lock;
	
	
	/**
	 * <p></p>
	 */
	private final IChunkManager chunk_manager;
	
	
	/**
	 * <p></p>
	 */
	private boolean is_consistency_checked;
	
	
	/**
	 * <p></p>
	 */
	private boolean is_consistent;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public FlakeDataManager(Flake flake, IChunkManager chunk_manager) {
		this(flake, chunk_manager, 0);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public FlakeDataManager(Flake flake, IChunkManager chunk_manager, int initial_chunk_list_size) {
		
		ArgumentChecker.checkForNull(chunk_manager, "chunk_manager");
		ArgumentChecker.checkForNull(flake, "flake");
		
		this.chunk_manager = chunk_manager;
		this.flake = flake;
		
		if( initial_chunk_list_size > 0 ) {
			chunk_list = new ArrayList<>(initial_chunk_list_size);
		}
		else {
			chunk_list = new ArrayList<>(0);
		}
		
		chunk_lock = new Object();
		length = 0;
		is_length_changed = true;
		is_consistency_checked = false;
		is_consistent = false;
		
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void addChunk(Chunk chunk) {
		if( chunk != null && chunk.isValid() ) {
			synchronized( chunk_lock ) {
				if( chunk_list.contains(chunk) ) {
					throw new SecurityException("The flake already contains this chunk: " + chunk.toString() + "!");
				}
				if( chunk_list.isEmpty() ) {
					chunk.setPositionInFlake(0);
				}
				else {
					Chunk last_chunk = chunk_list.get(chunk_list.size() - 1);
					chunk.setPositionInFlake(last_chunk.getPositionInFlake() + last_chunk.getLength());
				}
				chunk_list.add(chunk);
				chunk.save(flake);
				is_length_changed = true;
				is_consistency_checked = false;
			}
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void addChunks(Chunk[] chunks) {
		if( chunks != null && chunks.length > 0 ) {
			if( chunks.length > 1 ) {
				StreamFactory.getStream(chunks, StreamMode.Sequential).forEach(this::addChunk);
			}
			else {
				addChunk(chunks[0]);
			}
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void addChunks(Collection<Chunk> chunk_collection) {
		if( chunk_collection != null && chunk_collection.size() > 0 ) {
			StreamFactory.getStream(chunk_collection, StreamMode.Sequential).forEach(this::addChunk);
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void recycle() {
		synchronized( chunk_lock ) {
			chunk_manager.recycleChunks(chunk_list);
			chunk_list.clear();
			is_length_changed = true;
			is_consistency_checked = false;
		}
	}
	
	
	/**
	 * <p></p>
	 */
	private synchronized void countLength() {
		
		if( !isConsistent() ) {
			return;
		}
		
		synchronized( chunk_lock ) {
			if( is_length_changed ) {
				int chunk_list_size = chunk_list.size();
				if( chunk_list_size == 0 ) {
					length = 0;
				}
				else if( chunk_list_size == 1 ) {
					length = chunk_list.get(0).getLength();
				}
				else if( chunk_list_size == 2 ) {
					length = chunk_list.get(0).getLength() + chunk_list.get(1).getLength();
				}
				else {
					length = chunk_list.stream().mapToLong(chunk -> chunk.getLength()).sum();
				}
				is_length_changed = false;
			}
		}		
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public long getLength() {
		if( is_length_changed ) {
			countLength();
		}
		return length;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private void increaseLength(long difference) {
		chunk_manager.appendChunk(flake, difference);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private void decreaseLength(long difference) {
		
		long remaining_bytes = difference;
		
		synchronized( chunk_lock ) {
			
			Chunk buffer;
			Chunk last_chunk;
			
			do {
				
				buffer = chunk_list.remove(chunk_list.size() - 1);
				
				if( buffer.getLength() > remaining_bytes ) {
					
					buffer = chunk_manager.trimToSize(buffer, buffer.getLength() - remaining_bytes);
					
					if( chunk_list.isEmpty() ) {
						buffer.setPositionInFlake(0);
					}
					else {
						last_chunk = chunk_list.get(chunk_list.size() - 1);
						buffer.setPositionInFlake(last_chunk.getPositionInFlake() + last_chunk.getLength());
					}
					
					chunk_list.add(buffer);
					buffer.save(flake);
					remaining_bytes = 0;
					
				}
				else {
					remaining_bytes -= buffer.getLength();
					chunk_manager.recycleChunk(buffer);
				}
				
			}
			while( remaining_bytes > 0 );
			
			is_length_changed = true;
			is_consistency_checked = false;
			
		}
		
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void setLength(long new_length) {
		
		if( new_length < 0 ) {
			throw new IllegalArgumentException("The new_length must be smaller than 0!");
		}	
		else if( new_length == 0 ) {
			recycle();
		}
		
		long difference = new_length - getLength();
		
		if( difference == 0 ) {
			return;
		}
		else if( difference > 0 ) {
			increaseLength(difference);
		}
		else {
			// difference must be absoulte value
			// is here negative, so the unary "-" transforms the difference into its absolute value
			decreaseLength(-difference);			
		}
				
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public boolean isConsistent() {
		if( !is_consistency_checked ) {
			checkConsistency();
		}
		return is_consistent;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private synchronized void checkConsistency() {
		
		Chunk previous_chunk;
		Chunk current_chunk;
		int chunk_list_size;
		
		synchronized( chunk_lock ) {
			
			chunk_list_size = chunk_list.size();
			
			if( chunk_list_size == 0 ) {
				is_consistent = true;
				is_consistency_checked = true;
				return;
			}
			
			previous_chunk = chunk_list.get(0);
			
			if( previous_chunk == null || !previous_chunk.isValid() || previous_chunk.getPositionInFlake() != 0 ) {
				is_consistent = false;
				is_consistency_checked = true;
				return;
			}
			else if( chunk_list_size == 1 ) {
				is_consistent = true;
				is_consistency_checked = true;
				return;
			}
			
			for(int a=1;a<chunk_list_size;a++) {
				current_chunk = chunk_list.get(a);
				if( current_chunk == null || !current_chunk.isValid() || current_chunk.equals(previous_chunk) 
						|| !areNeighboursInFlake(current_chunk, previous_chunk) ) {
					is_consistent = false;
					is_consistency_checked = true;
					return;
				}
				previous_chunk = current_chunk;
			}
			
			is_consistent = true;
			is_consistency_checked = true;
						
		}
		
	}
	
	
	/**
	 * @return
	 */
	public int getNumberOfChunks() {
		synchronized( chunk_lock ) {
			return chunk_list.size();
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void mergeChunks() {
		
		if( getNumberOfChunks() < 2 ) {
			return;
		}
		
		Chunk current_chunk;
		Chunk following_chunk;
		int current_chunk_index = 0;
		int chunk_list_size;
		
		synchronized( chunk_lock ) {
			
			chunk_list_size = chunk_list.size();
			
			do {
				
				current_chunk = chunk_list.get(current_chunk_index);
				following_chunk = chunk_list.get(current_chunk_index + 1);
				
				if( current_chunk.isNeighbourOf(following_chunk) ) {
					// re-inserts the merged chunk at the position of the current_chunk
					chunk_list.add(
						current_chunk_index, 
						chunk_manager.mergeChunks(
								new Chunk[] {
									chunk_list.remove(current_chunk_index),
									// okay, because the elements in the chunk_list move one 
									// to the left after index current_chunk_index
									// so the following_chunk is now at position current_chunk_index
									chunk_list.remove(current_chunk_index)
								})
					);
					chunk_list_size--;
				}
				else {
					// only move the index to ensure that even a newly merged chunk
					// may be considered for merging with the following chunk
					current_chunk_index++;
				}
				
			}
			while( current_chunk_index < chunk_list_size - 1 );
			
			
			// fixes the position of the chunks inside the flake
			Chunk previous_chunk = chunk_list.get(0);
			previous_chunk.setPositionInFlake(0);
			previous_chunk.save(flake);
			
			for(int a=1;a<chunk_list_size;a++) {
				current_chunk = chunk_list.get(a);
				current_chunk.setPositionInFlake(previous_chunk.getPositionInFlake() + previous_chunk.getLength());
				current_chunk.save(flake);
				previous_chunk = current_chunk;
			}
			
		}
		
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public boolean needsChunkMerging() {
		synchronized( chunk_lock ) {
			for(int a=0,n=chunk_list.size()-1;a<n;a++) {
				if(chunk_list.get(a).isNeighbourOf(chunk_list.get(a+1))) {
					return true;
				}
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
	public IChunkInformation[] getChunks() {
		synchronized( chunk_lock ) {
			return chunk_list.toArray(new Chunk[0]);
		}
	}


	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public IChunkInformation getChunkAtIndex(int index) {
		synchronized( chunk_lock ) {
			ArgumentChecker.checkForBoundaries(index, 0, chunk_list.size() - 1, "index");
			return chunk_list.get(index);
		}
	}


	/**
	 * @param position_in_flake
	 * @return
	 */
	public IChunkInformation getChunkAtPosition(long position_in_flake) {
		
		ArgumentChecker.checkForBoundaries(position_in_flake, 0, getLength() - 1, "position_in_flake");
		int chunk_list_size;
		
		synchronized( chunk_lock ) {
			
			chunk_list_size = chunk_list.size();
			
			if( chunk_list_size == 1 ) {
				return chunk_list.get(0);
			}
			else if( chunk_list_size == 2 ) {
				if( chunk_list.get(0).getLength() > position_in_flake ) {
					return chunk_list.get(0);
				}
				else {
					return chunk_list.get(1);
				}
			}
			
		}
		
		if( chunk_list_size < FlakeDataManager.CHUNK_SEARCH_THRESHOLD ) {
			return getChunkAtPositionLinearSearch(position_in_flake);
		}
		else {
			return getChunkAtPositionBinarySearch(position_in_flake);
		}
		
	}


	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private IChunkInformation getChunkAtPositionLinearSearch(long position_in_flake) {
		
		ArgumentChecker.checkForBoundaries(position_in_flake, 0, getLength() - 1, "position_in_flake");
		
		synchronized( chunk_lock ) {
			
			for( Chunk chunk : chunk_list ) {
				
				if( 	(chunk != null) 
					&& 	(chunk.getPositionInFlake() <= position_in_flake) 
					&& 	(position_in_flake < ( chunk.getPositionInFlake() + chunk.getLength() )) ) {
					return chunk;
				}
			}
			
			throw new SecurityException("No chunk at the position \"" + position_in_flake + "\" was found!");
			
		}
		
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private IChunkInformation getChunkAtPositionBinarySearch(long position_in_flake) {
		
		if( !isConsistent() ) {
			return getChunkAtPositionLinearSearch(position_in_flake);
		}
		
		int left_index = 0;
		int right_index;
		int current_index;
		Chunk current_chunk;
		
		synchronized( chunk_lock ) {
			
			right_index = chunk_list.size() - 1;
			
			do {

				current_index = left_index + ((right_index - left_index) / 2);
				current_chunk = chunk_list.get(current_index);
				
				if( current_chunk.getPositionInFlake() < position_in_flake ) {
					if( current_chunk.containsFlakePosition(position_in_flake) ) {
						return current_chunk;
					}
					else {
						left_index = current_index;
					}
				}
				else {
					right_index = current_index;
				}
				
				if( right_index - left_index == 1 ) {
					current_chunk = chunk_list.get(right_index);
					if( current_chunk.containsFlakePosition(position_in_flake) ) {
						return current_chunk;
					}
					else {
						current_chunk = chunk_list.get(left_index);
						if( current_chunk.containsFlakePosition(position_in_flake) ) {
							return current_chunk;
						}
						else {
							break;
						}
					}
				}
				
			}
			while( left_index != right_index );
			
		}
		
		return getChunkAtPositionLinearSearch(position_in_flake);
		
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public int getIndexOfChunk(Chunk chunk) {
		synchronized( chunk_lock ) {
			return chunk_list.indexOf(chunk);
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void insertChunk(Chunk chunk, int index) {
		ArgumentChecker.checkForNull(chunk, "chunk");
		ArgumentChecker.checkForBoundaries(index, 0, Integer.MAX_VALUE, "index");
		synchronized( chunk_lock ) {
			if( chunk_list.contains(chunk) ) {
				throw new SecurityException("The flake already contains this chunk: " + chunk.toString() + "!");
			}
			if( index == chunk_list.size() ) {
				chunk_list.add(chunk);
			}
			else {
				if( index > chunk_list.size() ) {
					chunk_list.ensureCapacity(index + 1);
					do {
						chunk_list.add(null);
					}
					while( index >= chunk_list.size() );
				}
				Chunk replaced_chunk = chunk_list.set(index, chunk);
				if( replaced_chunk != null ) {
					throw new SecurityException("The replaced_chunk is not equal to null! " + replaced_chunk.toString());
				}
			}
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void arrangeChunks() {
		
		if( flake.hasBeenOpened() ) {
			throw new SecurityException("Can not order the chunks once the flake has been opened!");
		}
		
		long chunk_list_size;
		Chunk previous_chunk = null;
		
		synchronized( chunk_lock ) {
			
			chunk_list_size = chunk_list.size();
			
			if( chunk_list_size == 0 ) {
				return;
			}
			else if( chunk_list_size == 1 ) {
				chunk_list.get(0).setPositionInFlake(0);
				return;
			}
			
			for( Chunk chunk : chunk_list ) {
				if( previous_chunk == null ) {
					chunk.setPositionInFlake(0);
				}
				else {
					if( chunk == null ) {
						return;
					}
					else {
						chunk.setPositionInFlake( previous_chunk.getPositionInFlake() + previous_chunk.getLength() );
					}
				}
				previous_chunk = chunk;
			}
			
		}
		
	}

}
