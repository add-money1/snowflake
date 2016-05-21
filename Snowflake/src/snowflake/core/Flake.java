 package snowflake.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import j3l.util.check.ArgumentChecker;
import j3l.util.close.ClosureState;
import j3l.util.close.IClose;
import snowflake.api.FlakeInputStream;
import snowflake.api.FlakeOutputStream;
import snowflake.api.IFlake;
import snowflake.api.StorageException;
import snowflake.core.manager.IChannelManager;
import snowflake.core.manager.IChunkManager;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.05.20_0
 * @author Johannes B. Latzel
 */
public final class Flake implements IClose<IOException>, IFlake {
	
	
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
	private IChunkManager chunk_manager;
	
	
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
	 */
	private IChannelManager channel_manager;
	
	
	/**
	 * <p></p>
	 */
	private final long identification;
	
	
	/**
	 * <p></p>
	 */
	private ClosureState closure_state;
	
	
	/**
	 * <p></p>
	 */
	private boolean is_damaged;
	
	
	/**
	 * <p></p>
	 */
	private boolean is_deleted;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public Flake(long identification) {
		this.identification = identification;
		closure_state = ClosureState.None;
		chunk_list = new ArrayList<>(0);
		channel_manager = null;
		length = 0;
		is_damaged = false;
		is_deleted = false;
		is_consistency_checked = false;
		is_consistent = true;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void initialize(IChannelManager channel_manager, IChunkManager chunk_manager, ArrayList<Chunk> initial_chunk_list) {
		if( hasBeenOpened() ) {
			throw new SecurityException("Can not change the flake_stream_manager after the flake has been opened!");
		}
		this.channel_manager = ArgumentChecker.checkForNull(
				channel_manager, GlobalString.ChannelManager.toString()
		);
		this.chunk_manager = ArgumentChecker.checkForNull(chunk_manager, GlobalString.ChunkManager.toString());
		if( initial_chunk_list != null && !initial_chunk_list.isEmpty() ) {
			synchronized( chunk_list ) {
				chunk_list.addAll(initial_chunk_list);
			}
		}
		length = 0;
		synchronized( chunk_list ) {
			for( Chunk chunk : chunk_list ) {
				length += chunk.getLength();
			}
		}
		is_consistency_checked = false;
		is_consistent = false;	
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private synchronized void checkConsistency() {
		if( is_consistency_checked ) {
			return;
		}
		is_consistency_checked = true;
		if( (chunk_manager == null) || (channel_manager == null) ) {
			is_consistent = false;
			return;
		}
		Chunk previous_chunk;
		Chunk current_chunk;
		int chunk_list_size;
		synchronized( chunk_list ) {
			chunk_list_size = chunk_list.size();
			if( chunk_list_size == 0 ) {
				is_consistent = true;
				return;
			}
			previous_chunk = chunk_list.get(0);
			if( previous_chunk == null || !previous_chunk.isValid() || previous_chunk.getPositionInFlake() != 0 ) {
				is_consistent = false;
				return;
			}
			else if( chunk_list_size == 1 ) {
				is_consistent = true;
				return;
			}
			for(int a=1;a<chunk_list_size;a++) {
				current_chunk = chunk_list.get(a);
				if( current_chunk == null || !current_chunk.isValid() || current_chunk.equals(previous_chunk) 
						|| !areNeighboursInFlake(current_chunk, previous_chunk) ) {
					is_consistent = false;
					return;
				}
				previous_chunk = current_chunk;
			}		
		}
		is_consistent = true;
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
	public int getIndexOfChunk(Chunk chunk) {
		ArgumentChecker.checkForValidation(this);
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
	public void addChunks(Collection<Chunk> chunk_collection) {
		ArgumentChecker.checkForValidation(this);
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
						chunk.save(this);
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
	public void insertChunk(Chunk chunk, int index) {
		if( hasBeenOpened() ) {
			throw new SecurityException("The flake has already been opened!");
		}
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
	public void increaseLength(long difference) {
		chunk_manager.appendChunk(this, difference);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void decreaseLength(long difference) {
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
					buffer.save(this);
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
	
	
	/* (non-Javadoc)
	 * @see j3l.util.interfaces.IStateClosure#getClosureState()
	 */
	@Override public ClosureState getClosureState() {
		return closure_state;
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#delete()
	 */
	@Override public synchronized boolean delete() {
		if( !hasBeenOpened() ) {
			return false;
		}
		close();
		recycle();
		is_deleted = true;
		return true;
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#isValid()
	 */
	@Override public boolean isValid() {
		return !isDamaged() && !isDeleted() && isOpen() && isConsistent();
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#getLength()
	 */
	@Override public long getLength() {
		return length;
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#setLength(long)
	 */
	@Override public void setLength(long new_length) {
		ArgumentChecker.checkForValidation(this);
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
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#getNumberOfChunks()
	 */
	@Override public int getNumberOfChunks() {
		synchronized( chunk_list ) {
			return chunk_list.size();
		}
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#isDamaged()
	 */
	@Override public boolean isDamaged() {
		return is_damaged;
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#getChunks()
	 */
	@Override public IChunk[] getChunks() {
		synchronized( chunk_list ) {
			return chunk_list.toArray(new Chunk[0]);
		}
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#getChunkAtIndex(int)
	 */
	@Override public IChunk getChunkAtIndex(int index) {
		synchronized( chunk_list ) {
			return chunk_list.get(
				ArgumentChecker.checkForBoundaries(index, 0, chunk_list.size() - 1, GlobalString.Index.toString())
			);
		}
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#getChunkAtPositionInFlake(long)
	 */
	@Override public IChunk getChunkAtPositionInFlake(long position_in_flake) {
		ArgumentChecker.checkForValidation(this);
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
		throw new StorageException("The chunk at position \"" + position_in_flake + "\" can not be found!");
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#getFlakeInputStream()
	 */
	@Override public FlakeInputStream getFlakeInputStream() throws IOException {
		if( !isValid() ) {
			throw new SecurityException("The flake can not be streamed!");
		}
		return new FlakeInputStream(this, channel_manager.getChannel(), channel_manager);
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#getFlakeOutputStream()
	 */
	@Override public FlakeOutputStream getFlakeOutputStream() throws IOException {
		if( !isValid() ) {
			throw new SecurityException("The flake can not be streamed!");
		}
		return new FlakeOutputStream(this, channel_manager.getChannel(), channel_manager);
	}
	
	
	/* (non-Javadoc)
	 * @see j3l.util.interfaces.IClose#open()
	 */
	@Override public void open() {
		if( hasBeenOpened() ) {
			return;
		}
		closure_state = ClosureState.InOpening;
		long chunk_list_size;
		Chunk previous_chunk = null;
		synchronized( chunk_list ) {
			chunk_list_size = chunk_list.size();
			if( chunk_list_size == 1 && chunk_list.get(0) != null ) {
				chunk_list.get(0).setPositionInFlake(0);
			}
			else {
				for( Chunk chunk : chunk_list ) {
					if( previous_chunk == null ) {
						chunk.setPositionInFlake(0);
					}
					else {
						if( chunk == null ) {
							throw new StorageException("There must not be an empty chunk in the chunk_list!");
						}
						chunk.setPositionInFlake( previous_chunk.getPositionInFlake() + previous_chunk.getLength() );
					}
					previous_chunk = chunk;
				}
			}
		}
		is_consistency_checked = false;
		if( !isConsistent() ) {
			is_damaged = true;
		}
		closure_state = ClosureState.Open;
	}
	
	
	/* (non-Javadoc)
	 * @see j3l.util.interfaces.IClose#close()
	 */
	@Override public void close() {
		if( !isOpen() ) {
			return;
		}
		closure_state = ClosureState.Closed;
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override public String toString() {
		return "Flake[" + getIdentification() + "]";
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override public int hashCode() {
		return Long.hashCode(identification);
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override public boolean equals(Object object) {
		if( object != null && object instanceof IFlake ) {
			IFlake flake = (IFlake)object;
			if( flake.hashCode() == hashCode() && flake.getIdentification() == getIdentification() ) {
				return true;
			}
		}
		return false;
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#getIdentification()
	 */
	@Override public long getIdentification() {
		return identification;
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#isDeleted()
	 */
	@Override public boolean isDeleted() {
		return is_deleted;
	}
	
}
