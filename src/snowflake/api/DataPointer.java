package snowflake.api;

import j3l.util.Checker;
import snowflake.GlobalString;
import snowflake.StaticMode;
import snowflake.core.Flake;
import snowflake.core.IChunk;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.09.30_0
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
		if( StaticMode.TESTING_MODE ) {
			this.flake = Checker.checkForValidation(flake, GlobalString.Flake.toString());
		}
		else {
			this.flake = flake;
		}
		setPosition(position_in_flake);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private void checkEOF() {
		if( isEOF() ) {
			throw new SecurityException("Can not resolve the position of an eof-pointer!");
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public long getPositionInFlake() {
		checkEOF();
		return position_in_flake;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public long getPositionInStorage() {
		checkEOF();
		IChunk chunk = flake.getChunkAtPositionInFlake(position_in_flake);
		return chunk.getStartAddress() - chunk.getPositionInFlake() + position_in_flake;	
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public long getRemainingBytesInChunk() {
		if( isEOF() ) {
			return 0L;
		}
		IChunk chunk = flake.getChunkAtPositionInFlake(position_in_flake);
		return chunk.getLength() - ( position_in_flake - chunk.getPositionInFlake() );
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public long getRemainingBytes() {
		if( isEOF() ) {
			return 0L;
		}
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
		this.position_in_flake = Checker.checkForBoundaries(
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
		return super.hashCode();
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override public String toString() {
		StringBuilder string_builder = new StringBuilder(100);
		string_builder.append("pointer to flake \"");
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
