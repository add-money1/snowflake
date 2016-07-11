package snowflake.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import j3l.util.Checker;
import j3l.util.ClosureState;
import j3l.util.IClose;
import snowflake.GlobalString;
import snowflake.StaticMode;
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
 * @version 2016.07.11_0
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
		if( StaticMode.TESTING_MODE ) {
			this.channel_manager = Checker.checkForNull(
					channel_manager, GlobalString.ChannelManager.toString()
			);
			this.chunk_manager = Checker.checkForNull(chunk_manager, GlobalString.ChunkManager.toString());
		}
		else {
			this.channel_manager = channel_manager;
			this.chunk_manager = chunk_manager;
		}
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
				System.out.println((previous_chunk == null?"null": previous_chunk));
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
		if( StaticMode.TESTING_MODE ) {
			Checker.checkForValidation(this);
		}
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
	public void recycle() {
		synchronized( chunk_list ) {
			chunk_manager.recycleChunks(chunk_list);
			chunk_list.clear();
			length = 0;
			is_consistency_checked = false;
		}
	}
	
	
	/**
	 * <p>removes all chunks beginning with the chunk at position_in_flake</p>
	 * <p>
	 * 		guarantees that the order of the chunks remains and relies on the flake to
	 * 		add the chunks the chunk_list once the process which uses this method is
	 * 		done - does not save the chunks
	 * </p>
	 *
	 * @param position_in_flake position in flake
	 * @return Collection of the removed chunks
	 */
	private List<Chunk> removeAllChunksFrom(long position_in_flake) {
		ArrayList<Chunk> list;
		int left = 0;
		int right;
		int index;
		Chunk current_chunk;
		synchronized( chunk_list ) {
			Checker.checkForBoundaries(
				position_in_flake, 0, getLength(), GlobalString.PositionInFlake.toString()
			);
			right = chunk_list.size() - 1;
			do {
				index = left + (right - left) / 2;
				current_chunk = chunk_list.get(index);
				if( current_chunk.containsFlakePosition(position_in_flake) ) {
					break;
				}
				if( position_in_flake < current_chunk.getPositionInFlake() ) {
					right = index;
				}
				else {
					left = index + 1;
				}
			}
			while( true );
			list = new ArrayList<>( chunk_list.size() - index );
			for(int a=0;a<index;a++) {
				// does not need to increment the index since the removal will pull
				// the succeeding chunks to index
				current_chunk = chunk_list.remove(index);
				length -= current_chunk.getLength();
				list.add(current_chunk);
			}
			is_consistency_checked = false;
		}
		return list;
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
		Checker.checkForValidation(this);
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
			expandAtEnd(difference);
		}
		else {
			// difference must be absoulte value
			// is here negative, so the unary "-" transforms the difference into its absolute value
			cutFromEnd(-difference);			
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
			if( StaticMode.TESTING_MODE ) {
				return chunk_list.get(
					Checker.checkForBoundaries(index, 0, chunk_list.size() - 1, GlobalString.Index.toString())
				);
			}
			return chunk_list.get(index);
		}
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#getChunkAtPositionInFlake(long)
	 */
	@Override public IChunk getChunkAtPositionInFlake(long position_in_flake) {
		if( StaticMode.TESTING_MODE ) {
			Checker.checkForValidation(this);
			Checker.checkForBoundaries(
				position_in_flake, 0, getLength() - 1, GlobalString.PositionInFlake.toString()
			);
		}
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
		return new FlakeInputStream(this, channel_manager.getChannel(), channel_manager);
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#getFlakeOutputStream()
	 */
	@Override public FlakeOutputStream getFlakeOutputStream() throws IOException {
		return new FlakeOutputStream(this, channel_manager.getChannel(), channel_manager);
	}
	
	
	/* (non-Javadoc)
	 * @see j3l.util.interfaces.IClose#open()
	 */
	@Override public void open() {
		if( hasBeenOpened() ) {
			throw new StorageException("The flake has already been opened!");
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
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#cutFromStart(long)
	 */
	@Override public void cutFromStart(long number_of_bytes) {
		Chunk current_chunk;
		long remaining_bytes;
		synchronized( chunk_list ) {
			remaining_bytes = Checker.checkForBoundaries(
				number_of_bytes, 1, getLength(), GlobalString.NumberOfBytes.toString()
			);
			do {
				current_chunk = chunk_list.remove(0);
				length -= current_chunk.getLength();
				if( current_chunk.getLength() > remaining_bytes ) {
					SplitChunk split_chunk = chunk_manager.splitChunk(current_chunk, remaining_bytes);
					chunk_manager.recycleChunk(split_chunk.getLeftChunk());
					chunk_list.add(0, split_chunk.getRightChunk());
					length += split_chunk.getRightChunk().getLength();
					remaining_bytes = 0;
				}
				else {
					remaining_bytes -= current_chunk.getLength();
					chunk_manager.recycleChunk(current_chunk);
				}
			}
			while( remaining_bytes > 0 );
			current_chunk = chunk_list.get(0);
			current_chunk.setPositionInFlake(0);
			current_chunk.save(this);
			for(int a=1,n=chunk_list.size();a<n;a++) {
				chunk_list.get(a).setPositionInFlake(current_chunk.getPositionInFlake() + current_chunk.getLength());
				current_chunk = chunk_list.get(a);
				current_chunk.save(this);
			}
			is_consistency_checked = false;
		}
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#cutFromEnd(long)
	 */
	@Override public void cutFromEnd(long number_of_bytes) {
		Chunk current_chunk;
		long remaining_bytes;
		synchronized( chunk_list ) {
			remaining_bytes = Checker.checkForBoundaries(
				number_of_bytes, 1, getLength(), GlobalString.NumberOfBytes.toString()
			);
			do {
				current_chunk = chunk_list.remove( chunk_list.size() - 1 );
				length -= current_chunk.getLength();
				if( current_chunk.getLength() > remaining_bytes ) {
					chunk_manager.trimToSize(current_chunk, current_chunk.getLength() - remaining_bytes);
					chunk_list.add(current_chunk);
					current_chunk.save(this);
					length += current_chunk.getLength();
					remaining_bytes = 0;
				}
				else {
					remaining_bytes -= current_chunk.getLength();
					chunk_manager.recycleChunk(current_chunk);
				}
			}
			while( remaining_bytes > 0 );
			is_consistency_checked = false;
		}
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#cutAt(long, long)
	 */
	@Override public void cutAt(long position_in_flake, long number_of_bytes) {
		long remaining_bytes = Checker.checkForBoundaries(
			number_of_bytes, 1, getLength(), GlobalString.NumberOfBytes.toString()
		);
		List<Chunk> list = removeAllChunksFrom(position_in_flake);
		Chunk current_chunk;
		if( list.get(0).containsFlakePosition(position_in_flake) ) {
			current_chunk = list.remove(0);
			SplitChunk split_chunk = chunk_manager.splitChunk(
				current_chunk, current_chunk.getPositionInFlake() - position_in_flake
			);
			synchronized( chunk_list ) {
				chunk_list.add(split_chunk.getLeftChunk());
				if( chunk_list.isEmpty() ) {
					split_chunk.getLeftChunk().setPositionInFlake(0);
				}
				else {
					Chunk previous_chunk = chunk_list.get(chunk_list.size() - 1);
					split_chunk.getLeftChunk().setPositionInFlake(
						previous_chunk.getPositionInFlake() + previous_chunk.getLength()
					);
				}
			}
			list.add(0, split_chunk.getRightChunk());
		}
		do {
			current_chunk = list.remove(0);
			if( current_chunk.getLength() > remaining_bytes ) {
				SplitChunk split_chunk = chunk_manager.splitChunk(current_chunk, remaining_bytes);
				chunk_manager.recycleChunk(split_chunk.getLeftChunk());
				list.add(0, split_chunk.getRightChunk());
				remaining_bytes = 0;
			}
			else {
				remaining_bytes -= current_chunk.getLength();
				chunk_manager.recycleChunk(current_chunk);
			}
		}
		while( remaining_bytes > 0 );
		Chunk last_chunk;
		long length_change = 0;
		synchronized( chunk_list ) {
			if( chunk_list.isEmpty() ) {
				last_chunk = null;
			}
			else {
				last_chunk = chunk_list.get( chunk_list.size() - 1 );
			}
			for( Chunk chunk : list ) {
				length_change += chunk.getLength();
				if( last_chunk != null ) {
					chunk.setPositionInFlake( last_chunk.getPositionInFlake() + last_chunk.getLength() );
				}
				else {
					chunk.setPositionInFlake(0);
				}
				last_chunk = chunk;
			}
			chunk_list.addAll(list);
			length += length_change;
			is_consistency_checked = false;
		}
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#expandAtStart(long)
	 */
	@Override public void expandAtStart(long number_of_bytes) {
		Collection<Chunk> chunk_collection = chunk_manager.allocateSpace(number_of_bytes);
		Chunk previous_chunk, current_chunk;
		synchronized( chunk_list ) {
			for( Chunk chunk : chunk_collection ) {
				Checker.checkForValidation(chunk, GlobalString.Chunk.toString());
				if( chunk_list.contains(chunk) ) {
					throw new SecurityException("The flake already contains this chunk: " + chunk.toString() + "!");
				}
			}
			if( !chunk_list.isEmpty() ) {
				chunk_collection.addAll(chunk_list);
				chunk_list.clear();
			}
			chunk_list.addAll(chunk_collection);
			is_consistency_checked = false;
			previous_chunk = chunk_list.get(0);
			previous_chunk.setPositionInFlake(0);
			previous_chunk.save(this);
			for(int a=1,n=chunk_list.size();a<n;a++) {
				current_chunk = chunk_list.get(a);
				current_chunk.setPositionInFlake(previous_chunk.getPositionInFlake() + previous_chunk.getLength());
				current_chunk.save(this);
			}
		}
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#expandAtEnd(long)
	 */
	@Override public void expandAtEnd(long number_of_bytes) {
		Collection<Chunk> chunk_collection = chunk_manager.allocateSpace(number_of_bytes);
		Chunk last_chunk;
		synchronized( chunk_list ) {
			if( chunk_list.isEmpty() ) {
				last_chunk = null;
			}
			else {
				last_chunk = chunk_list.get( chunk_list.size() - 1 );
			}
			for( Chunk chunk : chunk_collection ) {
				Checker.checkForValidation(chunk, GlobalString.Chunk.toString());
				if( chunk_list.contains(chunk) ) {
					throw new SecurityException("The flake already contains this chunk: " + chunk.toString() + "!");
				}
				if( last_chunk != null ) {
					chunk.setPositionInFlake( last_chunk.getPositionInFlake() + last_chunk.getLength() );
				}
				else {
					chunk.setPositionInFlake(0);
				}
				chunk_list.add(chunk);
				length += chunk.getLength();
				chunk.save(this);
				is_consistency_checked = false;
				last_chunk = chunk;
			}
		}
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IFlake#expandAt(long, long)
	 */
	@Override public void expandAt(long position_in_flake, long number_of_bytes) {
		Checker.checkForBoundaries(
			number_of_bytes, 1, getLength(), GlobalString.NumberOfBytes.toString()
		);
		List<Chunk> list = removeAllChunksFrom(position_in_flake);
		Chunk current_chunk;
		if( !list.isEmpty() && list.get(0).containsFlakePosition(position_in_flake) ) {
			current_chunk = list.remove(0);
			SplitChunk split_chunk = chunk_manager.splitChunk(
				current_chunk, current_chunk.getPositionInFlake() - position_in_flake
			);
			synchronized( chunk_list ) {
				chunk_list.add(split_chunk.getLeftChunk());
				if( chunk_list.isEmpty() ) {
					split_chunk.getLeftChunk().setPositionInFlake(0);
				}
				else {
					Chunk previous_chunk = chunk_list.get(chunk_list.size() - 1);
					split_chunk.getLeftChunk().setPositionInFlake(
						previous_chunk.getPositionInFlake() + previous_chunk.getLength()
					);
				}
			}
			list.add(0, split_chunk.getRightChunk());
		}
		Collection<Chunk> collection = chunk_manager.allocateSpace(number_of_bytes);
		collection.addAll(list);
		synchronized( chunk_list ) {
			Chunk previous_chunk = null;
			if( !chunk_list.isEmpty() ) {
				previous_chunk = chunk_list.get( chunk_list.size() - 1 );
			}
			// must be added before chunk.save(this) is called
			chunk_list.addAll(collection);
			long length_change = 0;
			for( Chunk chunk : collection ) {
				if( previous_chunk == null ) {
					chunk.setPositionInFlake(0);
				}
				else {
					chunk.setPositionInFlake( previous_chunk.getPositionInFlake() + previous_chunk.getLength() );
				}
				chunk.save(this);
				length_change += chunk.getLength();
				previous_chunk = chunk;
			}
			length += length_change;
			is_consistency_checked = false;
		}
	}
	
}
