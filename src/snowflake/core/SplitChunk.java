package snowflake.core;

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
public final class SplitChunk {
	
	
	/**
	 * <p></p>
	 */
	private final Chunk left_chunk;
	
	
	/**
	 * <p></p>
	 */
	private final Chunk right_chunk;
	
	
	/**
	 * <p></p>
	 * 
	 * @param
	 */
	public SplitChunk(Chunk left_chunk, Chunk right_chunk) {
		if( StaticMode.TESTING_MODE ) {
			this.left_chunk = Checker.checkForValidation(left_chunk, GlobalString.LeftChunk.toString());
			this.right_chunk = Checker.checkForValidation(right_chunk, GlobalString.RightChunk.toString());
		}
		else {
			this.left_chunk = left_chunk;
			this.right_chunk = right_chunk;
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public Chunk getLeftChunk() {
		return left_chunk;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public Chunk getRightChunk() {
		return right_chunk;
	}

}
