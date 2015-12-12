package snowflake.api.chunk;

/**
 * <p>used for chunk-sorting</p>
 * 
 * @since JDK 1.8
 * @version 2015.10.19_0
 * @author Johannes B. Latzel
 */
public enum ChunkAttribute {
	
	
	/**
	 * <p>{@link snowflake.core.Chunk#getStartAddress()}</p>
	 */
	StartAddress,
	
	
	/**
	 * <p>{@link snowflake.core.Chunk#getPositionInFlake()}</p>
	 */
	PositionInFlake,
	
	
	/**
	 * <p>{@link snowflake.core.Chunk#getLength()}</p>
	 */
	Length;
	
}
