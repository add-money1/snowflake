package snowflake.core;

import java.io.IOException;

import j3l.util.Checker;
import snowflake.GlobalString;
import snowflake.StaticMode;
import snowflake.core.manager.IChunkMemory;


/**
 * <p>
 * 		A chunk represents a part of the stored data provided by the {@link snowflake.api.IStorage storage}. Its
 * 		start-address and its length uniquely identify a chunk. There won't be any other chunk with the same start-address.
 * </p>
 * <p>
 * 		A chunk includes the data in the {@link snowflake.api.IStorage storage} at the addresses from {@link #start_adress} to
 * 		{@link #start_adress} + {@link #length} - 1.
 * </p>
 * <p>
 * 		A chunk must never be part of more than one flake. If it would the data this chunk includes would be shared by more than
 * 		one {@link snowflake.api.IFlake flake} and could possibly cause irrepairable damage to the whole storage.
 * </p>
 * <p>
 * 		It can either be part of a {@link snowflake.api.IFlake flake} or be free. A free chunk will be managed and possibly
 * 		assigned to a {@link snowflake.api.IFlake flake} by the {@link snowflake.api.IStorage storage}.
 * </p>
 * 
 * @since JDK 1.8
 * @version 2016.07.11_0
 * @author Johannes B. Latzel
 */
public final class Chunk implements IChunk {
	
	
	
	/**
	 * <p>the unique start-address points directly to the begin-position of the data this chunk represents in the 
	 * {@link snowflake.api.IStorage storage}.</p>
	 */
	private final long start_address;
	
	
	/**
	 * <p>the amount of bytes this chunks includes</p>
	 */
	private final long length;
	
	
	/**
	 * <p>index in the chunk_table</p>
	 */
	private final long chunk_table_index;
	
	
	/**
	 * <p>the unique position inside the {@link snowflake.api.IFlake flake} - This position will always be unique
	 * for the specific flake.</p>
	 * <p>If this chunk is not part of a {@link snowflake.api.IFlake flake} than this field must have a negative value
	 * to state that it is a free chunk.</p>
	 */
	private long position_in_flake;
	
	
	/**
	 * <p>states if this chunk is valid</p>
	 */
	private boolean is_valid;
	
	
	/**
	 * <p>used for saving</p>
	 */
	private final IChunkMemory chunk_memory;
	
	
	/**
	 * <p>indicates if the chunk is currently marked for clearing</p>
	 */
	private boolean needs_to_be_cleared;
	
	
	/**
	 * <p>default initializes {@link #position_in_flake} with -1</p>
	 * 
	 * @param start_address initializer for {@link #start_adress} (must not be less than 0)
	 * @param length initializer for {@link #length} (must not be less than 0)
	 * @param table_index initializer for {@link #table_index} (must not be less than 0)
	 * @throws IllegalArgumentException
	 */
	public Chunk(IChunkMemory chunk_memory, long start_address, long length, long chunk_table_index) {
		if( StaticMode.TESTING_MODE ) {
			this.chunk_memory = Checker.checkForNull(chunk_memory, GlobalString.ChunkMemory.toString());
			this.start_address = Checker.checkForBoundaries(
				start_address, 0, Long.MAX_VALUE, GlobalString.StartAddress.toString()
			);
			this.length = Checker.checkForBoundaries(length, 1, Long.MAX_VALUE, GlobalString.Length.toString());
			this.chunk_table_index = Checker.checkForBoundaries(
				chunk_table_index, 0, Long.MAX_VALUE, GlobalString.ChunkTableIndex.toString()
			);
		}
		else {
			this.chunk_memory = chunk_memory;
			this.start_address = start_address;
			this.length = length;
			this.chunk_table_index = chunk_table_index;
		}
		position_in_flake = -1;
		is_valid = true;
		needs_to_be_cleared = false;
	}
	
	
	/**
	 * <p>states if this chunk is a logical neighbour to the parameter chunk</p>
	 * <p>Two chunks are logical neighbours if the start-address + length of one chunk is equal to the start-address
	 * of the other chunk.</p>
	 *
	 * @param chunk another chunk
	 * @return true if they are neighbour, false otherwise
	 */
	public boolean isNeighbourOf(Chunk chunk) {
		return ( getStartAddress() == (chunk.getStartAddress() + chunk.getLength()) ) || 
				( chunk.getStartAddress() == (getStartAddress() + getLength()) );
	}
	
	
	/**
	 * <p>assignes a new value to {@link #position_in_flake}</p>
	 *
	 * @param position_in_flake the new value for {@link #position_in_flake}
	 */
	public void setPositionInFlake(long position_in_flake) {
		this.position_in_flake = position_in_flake;
	}
	
	
	/**
	 * <p>resets the {@link #position_in_flake} to its default value -1</p>
	 */
	public void resetPositionInFlake() {
		position_in_flake = -1;
	}
	
	
	/**
	 * <p>tests the {@link #position_in_flake} for positivity (e.g. if this chunk is part of a  {@link #position_in_flake})</p>
	 *
	 * @return true if it is part of a {@link snowflake.api.IFlake flake}, false otherwise
	 */
	public boolean isPartOfFlake() {
		return position_in_flake >= 0;
	}
	
	
	/**
	 * <p>	<li>deletes the related data</li>
	 *		<li>invalidates this instance</li>
	 * </p>
	 * @throws IOException 
	 */
	public void delete() {
		
		if( !isValid() ) {
			return;
		}

		chunk_memory.deleteChunk(this);
		is_valid = false;
		
	}
	
	
	/**
	 * <p>saves this chunk</p>
	 *
	 * @param owner_flake the flake which owns this chunk or null, if this chunks is available
	 */
	public synchronized void save(Flake owner_flake) {
		chunk_memory.saveChunk(owner_flake, this);
	}
	
	
	/**
	 * @return {@link #needs_to_be_cleared}
	 */
	public boolean needsToBeCleared() {
		return needs_to_be_cleared;
	}
	
	
	/**
	 * <p>setter for {@link #needs_to_be_cleared}</p>
	 *
	 * @param needs_to_be_cleared see {@link #needs_to_be_cleared}
	 */
	public void setNeedsToBeCleared(boolean needs_to_be_cleared) {
		this.needs_to_be_cleared = needs_to_be_cleared;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public boolean containsFlakePosition(long position) {
		if( getPositionInFlake() < 0 ) {
			return false;
		}
		return (getPositionInFlake() <= position) && (position < (getPositionInFlake() + getLength()));
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see snowflake.api.IChunk#getStartAddress()
	 */
	@Override public long getStartAddress() {
		return start_address;
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see snowflake.api.IChunk#getLength()
	 */
	@Override public long getLength() {
		return length;
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see snowflake.api.IChunk#getPositionInFlake()
	 */
	@Override public long getPositionInFlake() {
		return position_in_flake;
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override public boolean equals(Object o) {
		if( ( o != null ) && ( o instanceof Chunk ) && ( o.hashCode() == hashCode() ) ) {
			Chunk chunk = (Chunk)o;
			return 		(chunk.getStartAddress() == getStartAddress())
					&&  (chunk.getLength() == getLength())
					&&  (chunk.getChunkTableIndex() == getChunkTableIndex());
		}
		return false;
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override public int hashCode() {
		return Long.hashCode(getChunkTableIndex());
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override public String toString() {
		StringBuilder string_builder = new StringBuilder(90);
		string_builder.append("[start_address: ");
		string_builder.append(Long.toString(getStartAddress()));
		string_builder.append(" | length: ");
		string_builder.append(Long.toString(getLength()));
		string_builder.append(" | position_in_flake: ");
		string_builder.append(Long.toString(getPositionInFlake()));
		string_builder.append(" | chunk_table_index: ");
		string_builder.append(Long.toString(getChunkTableIndex()));
		string_builder.append("]");
		string_builder.trimToSize();
		return string_builder.toString();
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IChunkInformation#getTableIndex()
	 */
	@Override public long getChunkTableIndex() {
		return chunk_table_index;
	}


	/*
	 * (non-Javadoc)
	 * @see snowflake.api.chunk.IChunkInformation#isValid()
	 */
	@Override public boolean isValid() {
		return is_valid && ( getLength() > 0 ) && ( getStartAddress() >= 0 ) && ( getChunkTableIndex() >= 0 );
	}
	

}
