package snowflake.core;

import j3l.util.IValidate;

/**
 * <p>provides read-only informations of a Chunk</p>
 * 
 * @since JDK 1.8
 * @version 2016.07.11_0
 * @author Johannes B. Latzel
 */
public interface IChunk extends IValidate {
	
	
	/**
	 * <p>returns the position of this chunk in its corresponding flake</p>
	 *  
	 * @return position in flake
	 */
	long getPositionInFlake();
	
	
	/**
	 * @return {@link #start_adress}
	 */
	long getStartAddress();
	
	
	/**
	 * @return {@link #length}
	 */
	long getLength();	
	
	
	/**
	 * @return {@link #chunk_table_index}
	 */
	long getChunkTableIndex();
	
	
	/**
	 * <p>states if this chunk is valid - an invalid chunk must not be used in any way!</p>
	 *
	 * @return true, if the chunk is valid, else otherwise
	 */
	@Override boolean isValid();
	
}
