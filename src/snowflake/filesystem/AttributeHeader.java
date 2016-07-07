package snowflake.filesystem;

import j3l.util.check.ArgumentChecker;
import snowflake.GlobalString;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.06.06_0
 * @author Johannes B. Latzel
 */
public final class AttributeHeader {
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private final static int UNSIGNED_SHORT_OFFSET = 1 << 15;
	
	
	/**
	 * <p></p>
	 */
	private final short name_length;
	
	
	/**
	 * <p></p>
	 */
	private final short type_name_length;
	
	
	/**
	 * <p></p>
	 */
	private final int value_length;
	
	
	/**
	 * <p></p>
	 */
	private final long last_changed_time_stamp;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public AttributeHeader(short name_length, short type_name_length, int value_length, long last_changed_time_stamp) {
		this.name_length = name_length;
		this.type_name_length = type_name_length;
		this.value_length = ArgumentChecker.checkForBoundaries(
			value_length, 0, Integer.MAX_VALUE, GlobalString.ValueLength.toString()
		);
		this.last_changed_time_stamp = ArgumentChecker.checkForBoundaries(
			last_changed_time_stamp, 0, Long.MAX_VALUE, GlobalString.LastChangedTimeStamp.toString()
		);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public int getNameLength() {
		return name_length + AttributeHeader.UNSIGNED_SHORT_OFFSET;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public int getTypeNameLength() {
		return type_name_length + AttributeHeader.UNSIGNED_SHORT_OFFSET;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public int getValueLength() {
		return value_length;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public long getLastChangedTimeStamp() {
		return last_changed_time_stamp;
	}
	
}
