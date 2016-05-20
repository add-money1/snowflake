package snowflake.core.flake;

import java.util.ArrayList;
import java.util.Collection;

import j3l.util.check.ArgumentChecker;
import snowflake.api.StorageException;
import snowflake.core.Chunk;
import snowflake.core.GlobalString;
import snowflake.core.IChunk;
import snowflake.core.manager.IChunkManager;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.05.16_0
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
	private final ArrayList<Chunk> chunk_list;
	
	
	/**
	 * <p></p>
	 */
	private final Flake flake;
	
	
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
		this.chunk_manager = ArgumentChecker.checkForNull(chunk_manager, GlobalString.ChunkManager.toString());
		this.flake = ArgumentChecker.checkForNull(flake, GlobalString.Flake.toString());
		if( initial_chunk_list_size > 0 ) {
			chunk_list = new ArrayList<>(initial_chunk_list_size);
		}
		else {
			chunk_list = new ArrayList<>(0);
		}
		length = 0;
		is_consistency_checked = false;
		is_consistent = false;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void addChunks(Collection<Chunk> chunk_collection) {
		if( chunk_collection != null && chunk_collection.size() > 0 ) {
			synchronized( chunk_list ) {
				for( Chunk chunk : chunk_collection ) {
					if( chunk != null && chunk.isValid() ) {
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
						length += chunk.getLength();
						chunk.save(flake);
					}
					else {
						throw new StorageException("Can not add the chunk \"" + chunk + "\": chunk is either null or invalid!");
					}
				}
			}
			is_consistency_checked = false;
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void setInitialChunks(ArrayList<Chunk> initial_chunk_list) {
		// risky, because the method could be called any time, but must only be called before the flake is opened
		if( initial_chunk_list != null && initial_chunk_list.size() > 0 ) {
			Chunk current_chunk;
			synchronized( chunk_list ) {
				for(int a=0,n=initial_chunk_list.size();a<n;a++) {
					current_chunk = initial_chunk_list.get(a);
					if( current_chunk != null && chunk_list.contains(current_chunk) ) {
						throw new SecurityException("The flake already contains this chunk: " + current_chunk + "!");
					}
					chunk_list.add(current_chunk);
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
	public void recycle() {
		synchronized( chunk_list ) {
			chunk_manager.recycleChunks(chunk_list);
			chunk_list.clear();
			length = 0;
			is_consistency_checked = false;
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public long getLength() {
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
		long remaining_bytes = (difference > length) ? length : difference;
		synchronized( chunk_list ) {
			Chunk buffer;
			Chunk last_chunk;
			length -= remaining_bytes;
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
		
		synchronized( chunk_list ) {
			
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
		synchronized( chunk_list ) {
			return chunk_list.size();
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public IChunk[] getChunks() {
		synchronized( chunk_list ) {
			return chunk_list.toArray(new Chunk[0]);
		}
	}


	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public IChunk getChunkAtIndex(int index) {
		synchronized( chunk_list ) {
			return chunk_list.get(
				ArgumentChecker.checkForBoundaries(index, 0, chunk_list.size() - 1, GlobalString.Index.toString())
			);
		}
	}


	/**
	 * @param position_in_flake
	 * @return
	 */
	public IChunk getChunkAtPosition(long position_in_flake) {
		
		ArgumentChecker.checkForBoundaries(position_in_flake, 0, getLength() - 1, GlobalString.PositionInFlake.toString());
		int chunk_list_size;
		
		synchronized( chunk_list ) {
			
			chunk_list_size = chunk_list.size();
			
			if( chunk_list_size == 1 ) {
				return chunk_list.get(0);
			}
			else if( chunk_list_size == 2 ) {
				if( chunk_list.get(0).getLength() > position_in_flake ) {
					return chunk_list.get(0);
				}
				return chunk_list.get(1);
			}
			
		}
		
		if( chunk_list_size < FlakeDataManager.CHUNK_SEARCH_THRESHOLD ) {
			return getChunkAtPositionLinearSearch(position_in_flake);
		}
		return getChunkAtPositionBinarySearch(position_in_flake);
		
	}


	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private IChunk getChunkAtPositionLinearSearch(long position_in_flake) {
		
		ArgumentChecker.checkForBoundaries(position_in_flake, 0, getLength() - 1, GlobalString.PositionInFlake.toString());
		
		synchronized( chunk_list ) {
			
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
	private IChunk getChunkAtPositionBinarySearch(long position_in_flake) {
		
		if( !isConsistent() ) {
			return getChunkAtPositionLinearSearch(position_in_flake);
		}
		
		int left_index = 0;
		int right_index;
		int current_index;
		Chunk current_chunk;
		
		synchronized( chunk_list ) {
			
			right_index = chunk_list.size() - 1;
			
			do {

				current_index = left_index + ((right_index - left_index) / 2);
				current_chunk = chunk_list.get(current_index);
				
				if( current_chunk.getPositionInFlake() < position_in_flake ) {
					if( current_chunk.containsFlakePosition(position_in_flake) ) {
						return current_chunk;
					}
					left_index = current_index;
				}
				else {
					right_index = current_index;
				}
				
				if( right_index - left_index == 1 ) {
					current_chunk = chunk_list.get(right_index);
					if( current_chunk.containsFlakePosition(position_in_flake) ) {
						return current_chunk;
					}
					current_chunk = chunk_list.get(left_index);
					if( current_chunk.containsFlakePosition(position_in_flake) ) {
						return current_chunk;
					}
					break;
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
		synchronized( chunk_list ) {
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
		ArgumentChecker.checkForNull(chunk, GlobalString.Chunk.toString());
		ArgumentChecker.checkForBoundaries(index, 0, Integer.MAX_VALUE, GlobalString.Index.toString());
		synchronized( chunk_list ) {
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
		
		synchronized( chunk_list ) {
			
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
					chunk.setPositionInFlake( previous_chunk.getPositionInFlake() + previous_chunk.getLength() );
				}
				previous_chunk = chunk;
			}
			
		}
		
	}

}
