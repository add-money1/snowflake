package snowflake.filesystem;

import java.nio.ByteBuffer;

import j3l.util.Checker;
import snowflake.GlobalString;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.09.22_0
 * @author Johannes B. Latzel
 */
public final class AttributeHeader {
	
	
	/**
	 * <p></p>
	 */
	public static AttributeHeader create(ByteBuffer header_buffer) {
		short name_length = header_buffer.getShort();
		short type_name_length = header_buffer.getShort();
		int value_length = header_buffer.getInt();
		return AttributeHeader.create(name_length, type_name_length, value_length);
	}
	
	
	/**
	 * <p></p>
	 */
	public static AttributeHeader create(short name_length, short type_name_length, int value_length) {
		return new AttributeHeader(name_length, type_name_length, value_length);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private final static int UNSIGNED_SHORT_OFFSET = 1 << (Short.BYTES * 2 - 1);
	
	
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
	 *
	 * @param
	 * @return
	 */
	private AttributeHeader(short name_length, short type_name_length, int value_length) {
		this.name_length = name_length;
		this.type_name_length = type_name_length;
		this.value_length = Checker.checkForBoundaries(
			value_length, 0, Integer.MAX_VALUE, GlobalString.ValueLength.toString()
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
	
}
