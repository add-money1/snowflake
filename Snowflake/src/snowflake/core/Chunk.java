package snowflake.core;

import java.io.IOException;

import j3l.util.check.ArgumentChecker;
import snowflake.api.chunk.IChunkInformation;
import snowflake.api.chunk.IChunkMemory;
import snowflake.core.flake.Flake;


/**
 * <p>A chunk represents a part of the stored data provided by the {@link snowflake.api.IStorage storage}. Its
 * start-address and its length uniquely identify a chunk. There won't be any other chunk with the same start-address.</p>
 * <p>A chunk includes the data in the {@link snowflake.api.IStorage storage} at the addresses from {@link #start_adress} to
 * {@link #start_adress} + {@link #length} - 1.</p>
 * <p>A chunk must never be part of more than one flake. If it would the data this chunk includes would be shared by more than
 * one {@link snowflake.api.flake.IFlake flake} and could possibly cause irrepairable damage to the whole storage.</p>
 * <p>It can either be part of a {@link snowflake.api.flake.IFlake flake} or be free. A free chunk will be managed and possibly
 * assigned to a {@link snowflake.api.flake.IFlake flake} by the {@link snowflake.api.IStorage storage}.</p>
 * 
 * @since JDK 1.8
 * @version 2015.12.12_0
 * @author Johannes B. Latzel
 */
public final class Chunk implements IChunkInformation {
	
	
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
	 * <p>the unique position inside the {@link snowflake.api.flake.IFlake flake} - This position will always be unique
	 * for the specific flake.</p>
	 * <p>If this chunk is not part of a {@link snowflake.api.flake.IFlake flake} than this field must have a negative value
	 * to state that it is a free chunk.</p>
	 */
	private long position_in_flake;
	
	
	/**
	 * <p>states if this chunk is valid</p>
	 */
	private boolean is_valid;
	
	
	/**
	 * <p></p>
	 */
	private final IChunkMemory chunk_memory;
	
	
	/**
	 * <p></p>
	 */
	private boolean needs_to_be_saved;
	
	
	/**
	 * <p>default initializes {@link #position_in_flake} with -1</p>
	 * 
	 * @param start_address initializer for {@link #start_adress} (must not be less than 0)
	 * @param length initializer for {@link #length} (must not be less than 0)
	 * @param table_index initializer for {@link #table_index} (must not be less than 0)
	 * @throws IllegalArgumentException
	 */
	public Chunk(IChunkMemory chunk_memory, long start_address, long length, long chunk_table_index) {

		ArgumentChecker.checkForNull(chunk_memory, "chunk_memory");
		ArgumentChecker.checkForBoundaries(start_address, 0, Long.MAX_VALUE, "start_address");
		ArgumentChecker.checkForBoundaries(length, 1, Long.MAX_VALUE, "length");
		ArgumentChecker.checkForBoundaries(chunk_table_index, 0, Long.MAX_VALUE, "chunk_table_index");

		this.chunk_memory = chunk_memory;
		this.start_address = start_address;
		this.length = length;
		this.chunk_table_index = chunk_table_index;
		position_in_flake = -1;
		is_valid = true;
		needs_to_be_saved = false;
		
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
	
	
	/*
	 * (non-Javadoc)
	 * @see snowflake.api.IChunk#getPositionInFlake()
	 */
	@Override public long getPositionInFlake() {
		return position_in_flake;
	}
	
	
	/**
	 * <p>assignes a new value to {@link #position_in_flake}</p>
	 *
	 * @param position_in_flake the new value for {@link #position_in_flake}
	 */
	public void setPositionInFlake(long position_in_flake) {
		this.position_in_flake = position_in_flake;
		needs_to_be_saved = true;
	}
	
	
	/**
	 * <p>resets the {@link #position_in_flake} to its default value -1</p>
	 */
	public void resetPositionInFlake() {
		position_in_flake = -1;
		needs_to_be_saved = true;
	}
	
	
	/**
	 * <p>tests the {@link #position_in_flake} for positivity (e.g. if this chunk is part of a  {@link #position_in_flake})</p>
	 *
	 * @return true if it is part of a {@link snowflake.api.flake.IFlake flake}, false otherwise
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
		
		is_valid = false;
		needs_to_be_saved = false;
		chunk_memory.deleteChunk(this);
		
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void save(Flake owner_flake) {
		if( needsToBeSaved() ) {
			chunk_memory.saveChunk(owner_flake, this);
		}
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override public boolean equals(Object o) {
		
		if( ( o != null ) && ( o instanceof Chunk ) && ( o.hashCode() == hashCode() ) ) {
			
			Chunk chunk = (Chunk)o;
			return ( chunk.getStartAddress() == getStartAddress() ) && ( chunk.getLength() == getLength() );
			
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
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IChunkInformationrmation#containsStoragePosition(long)
	 */
	@Override public boolean containsStoragePosition(long position_in_storage) {
		return (getStartAddress() <= position_in_storage) && (position_in_storage < (getStartAddress() + getLength()));
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IChunkInformationrmation#containsFlakePosition(long)
	 */
	@Override public boolean containsFlakePosition(long position) {
		if( getPositionInFlake() < 0 ) {
			return false;
		}
		return (getPositionInFlake() <= position) && (position < (getPositionInFlake() + getLength()));
	}


	/* (non-Javadoc)
	 * @see snowflake.api.IChunkInformationrmation#needsToBeSaved()
	 */
	@Override public boolean needsToBeSaved() {
		return needs_to_be_saved;
	}
	

}
