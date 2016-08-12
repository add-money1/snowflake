package snowflake.api;

import j3l.util.Checker;
import snowflake.GlobalString;
import snowflake.StaticMode;
import snowflake.filesystem.File;
import snowflake.filesystem.manager.DeduplicationBlock;
import snowflake.filesystem.manager.IDeduplicationDescription;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.08.12_0
 * @author Johannes B. Latzel
 */
public final class DeduplicationDataPointer {
	
	
	/**
	 * <p></p>
	 */
	private final IDeduplicationDescription deduplication_description;
	
	
	/**
	 * <p></p>
	 */
	private long position_in_file;
	
	
	/**
	 * <p></p>
	 */
	private final long eof_pointer;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public DeduplicationDataPointer(File file) {
		if( StaticMode.TESTING_MODE ) {
			Checker.checkForNull(file, GlobalString.File.toString());
			this.deduplication_description = Checker.checkForNull(
				file.getDeduplicationDescription(), GlobalString.DeduplicationDescription.toString()
			);
		}
		else {
			this.deduplication_description = file.getDeduplicationDescription();
		}
		position_in_file = 0;
		eof_pointer = file.getLength() - deduplication_description.getEndOfDeduplicationPointer()
				+ deduplication_description.getDeduplicationLevel() * DeduplicationBlock.SIZE;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public long getPositionInFile() {
		return position_in_file;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public long getDeduplicationIndexPosition() {
		long index_position = position_in_file;
		for(int a=0,n=deduplication_description.getDeduplicationLevel();a<n;a++) {
			index_position /= DeduplicationBlock.SIZE;
		}
		return index_position;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public boolean pointsToDeduplicatedData() {
		return position_in_file < deduplication_description.getEndOfDeduplicationPointer();
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public long getAvailableBytes() {
		return eof_pointer;
	}
	
}
