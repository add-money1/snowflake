package snowflake.api.flake;

import j3l.util.check.ArgumentChecker;
import snowflake.api.GlobalString;
import snowflake.api.chunk.IChunkInformation;
import snowflake.core.flake.Flake;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.03.10_0
 * @author Johannes B. Latzel
 */
public final class DataPointer {	
	
	
	/**
	 * <p></p>
	 */
	private final Flake flake;
	
	
	/**
	 * <p></p>
	 */
	private long position_in_flake;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public DataPointer(Flake flake, long position_in_flake) {
		this.flake = ArgumentChecker.checkForNull(flake, GlobalString.Flake.toString());
		setPosition(
			ArgumentChecker.checkForBoundaries(position_in_flake, 0, Long.MAX_VALUE, GlobalString.PositionInFlake.toString())
		);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public long getPositionInFlake() {
		return position_in_flake;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public long getPositionInStorage() {
		
		if( isEOF() ) {
			throw new SecurityException("Can not resolve the position of an eof-pointer!");
		}
		
		IChunkInformation chunk = flake.getChunkAtPositionInFlake(position_in_flake);
		
		long position_in_storage = chunk.getStartAddress()
				+ ( position_in_flake - chunk.getPositionInFlake() );
		
		if( position_in_storage < 0 ) {
			throw new SecurityException(
				"The position_in_storage managed to overflow!" 
				+ " [chunk.getStartAddress(): " + chunk.getStartAddress()
				+ " | pointer_position_in_flake: " + position_in_flake
				+ " | chunk_position_in_flake: " + chunk.getPositionInFlake() + "]"
			);
		}
		
		return position_in_storage;	
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public long getRemainingBytesInChunk() {
		IChunkInformation chunk = flake.getChunkAtPositionInFlake(position_in_flake);
		return chunk.getLength() - ( position_in_flake - chunk.getPositionInFlake() );
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public long getRemainingBytes() {
		return flake.getLength() - position_in_flake;
	}
	
	
	/**
	 * <p>increases the position in the flake by 1</p>
	 */
	public void increasePosition() {
		position_in_flake++;
	}
	
	
	/**
	 * <p>decreases the position in the flake by one</p>
	 */
	public void decreasePosition() {
		position_in_flake--;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void setPosition(long position_in_flake) {
		this.position_in_flake = ArgumentChecker.checkForBoundaries(
			position_in_flake, 0, flake.getLength(), GlobalString.PositionInFlake.toString()
		);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void changePosition(long delta) {
		if( delta != 0 ) {
			setPosition( position_in_flake + delta );
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public boolean isEOF() {
		return getRemainingBytes() == 0;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void seekEOF() {
		setPosition(flake.getLength());
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override public DataPointer clone() {
		return new DataPointer(flake, position_in_flake);
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override public int hashCode() {
		return Long.hashCode(getPositionInStorage());
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override public String toString() {
		
		StringBuilder string_builder = new StringBuilder(100);
		
		string_builder.append("pointer in flake \"");
		string_builder.append(flake.toString());
		string_builder.append("\" at position: ");
		string_builder.append(position_in_flake);
		string_builder.trimToSize();
		
		return string_builder.toString();
		
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override public boolean equals(Object object) {
		
		if( object instanceof DataPointer ) {
			
			DataPointer data_pointer = (DataPointer)object;
			
			if( isEOF() != data_pointer.isEOF() ) {
				return false;
			}
			
			return getPositionInStorage() == data_pointer.getPositionInStorage();		
		}
		
		return false;
		
	}
	
}
