package snowflake.filesystem.manager;

import j3l.util.Checker;
import snowflake.GlobalString;
import snowflake.StaticMode;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.07.12_0
 * @author Johannes B. Latzel
 */
public class DeduplicationDescription {
	
	
	/**
	 * <p></p>
	 */
	private final byte deduplication_level;
	
	
	/**
	 * <p></p>
	 */
	private final long end_of_deduplication_pointer;
	
	
	/**
	 * <p></p>
	 * 
	 * @param
	 */
	public DeduplicationDescription(byte deduplication_level, long end_of_deduplication_pointer) {
		if( StaticMode.TESTING_MODE ) {
			this.deduplication_level = Checker.checkForBoundaries(
				deduplication_level, (byte)0, Byte.MAX_VALUE, GlobalString.DeduplicationLevel.toString()
			);
			this.end_of_deduplication_pointer = Checker.checkForBoundaries(
				end_of_deduplication_pointer, 0, Long.MAX_VALUE, GlobalString.EndOfDeduplicationPointer.toString()
			);
		}
		else {
			this.deduplication_level = deduplication_level;
			this.end_of_deduplication_pointer = end_of_deduplication_pointer;
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public byte getDeduplicationLevel() {
		return deduplication_level;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public long getEndOfDeduplicationPointer() {
		return end_of_deduplication_pointer;
	}
	
}
