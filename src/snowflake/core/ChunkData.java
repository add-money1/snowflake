package snowflake.core;

import j3l.util.IBinaryData;
import j3l.util.Checker;
import snowflake.GlobalString;
import snowflake.StaticMode;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.07.11_0
 * @author Johannes B. Latzel
 */
public final class ChunkData implements IBinaryData {
	
	
	/**
	 * <p></p>
	 */
	private final long start_address;
	
	
	/**
	 * <p></p>
	 */
	private final long length;
	
	
	/**
	 * <p></p>
	 */
	private final long flake_identification;
	
	
	/**
	 * <p></p>
	 */
	private final int index_in_flake;
	
	
	/**
	 * <p></p>
	 */
	private final byte flag_vector;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public ChunkData(long start_address, long length, long flake_indentification, int index_in_flake, byte flag_vector) {
		if( StaticMode.TESTING_MODE ) {
			this.start_address = Checker.checkForBoundaries(
				start_address, 0, Long.MAX_VALUE, GlobalString.StartAddress.toString()
			);
			this.length = Checker.checkForBoundaries(length, 0, Long.MAX_VALUE, GlobalString.Length.toString());
			this.index_in_flake = Checker.checkForBoundaries(
				index_in_flake, 0, Integer.MAX_VALUE, GlobalString.IndexInFlake.toString()
			);
		}
		else {
			this.start_address = start_address;
			this.length = length;
			this.index_in_flake = index_in_flake;
		}
		this.flake_identification = flake_indentification;
		this.flag_vector = flag_vector;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public long getStartAddress() {
		return start_address;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public long getChunkLength() {
		return length;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public long getFlakeIdentification() {
		return flake_identification;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public int getIndexInFlake() {
		return index_in_flake;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public byte getFlagVector() {
		return flag_vector;
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.core.data.IBinaryData#getBinaryData(byte[])
	 */
	@Override public void getBinaryData(byte[] buffer) {
		ChunkUtility.getBinaryData(this, buffer);
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.core.data.IBinaryData#getDataLength()
	 */
	@Override public int getDataLength() {
		return ChunkUtility.BINARY_CHUNK_SIZE;
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override public String toString() {
		StringBuilder string_builder = new StringBuilder(130);
		string_builder.append("ChunkData: [start_address = ");
		string_builder.append(getStartAddress());
		string_builder.append(" | length = ");
		string_builder.append(getChunkLength());
		string_builder.append(" | flake_identification = ");
		string_builder.append(getFlakeIdentification());
		string_builder.append(" | index_in_flake = ");
		string_builder.append(getIndexInFlake());
		string_builder.append(" | flag_vector = ");
		string_builder.append(getFlagVector());
		string_builder.append("]");
		string_builder.trimToSize();
		return string_builder.toString();
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override public int hashCode() {
		return Long.hashCode(getStartAddress() ^ Long.rotateLeft(getChunkLength(), 32));
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override public boolean equals(Object object) {
		if( object != null && object instanceof ChunkData ) {
			ChunkData chunk_data = (ChunkData)object;
			if( chunk_data.hashCode() == hashCode() ) {
				return chunk_data.getStartAddress() == getStartAddress() && chunk_data.getChunkLength() == getChunkLength();
			}
		}
		return false;
	}
	
}
