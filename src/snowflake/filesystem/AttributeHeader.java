package snowflake.filesystem;

import java.nio.ByteBuffer;

import j3l.util.Checker;
import snowflake.GlobalString;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.09.29_0
 * @author Johannes B. Latzel
 */
public final class AttributeHeader {
	
	
	/**
	 * <p></p>
	 */
	public final static int SIZE = 2 * Short.BYTES + Integer.BYTES;
	
	
	/**
	 * <p></p>
	 */
	public static AttributeHeader create(ByteBuffer header_buffer) {
		Checker.checkForBoundaries(
			header_buffer.remaining(),
			AttributeHeader.SIZE,
			AttributeHeader.SIZE,
			GlobalString.BufferLength.toString()
		);
		return AttributeHeader.create(header_buffer.getShort(), header_buffer.getShort(), header_buffer.getInt());
	}
	
	
	/**
	 * <p></p>
	 */
	public static AttributeHeader create(short name_length, short type_name_length, int value_length) {
		System.out.println("header: " + name_length + " " + type_name_length + " " + value_length);
		return new AttributeHeader(name_length, type_name_length, value_length);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public static ByteBuffer createBuffer() {
		return ByteBuffer.allocateDirect(AttributeHeader.SIZE);
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
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public int getAttributeLength() {
		return AttributeHeader.SIZE + getNameLength() + getTypeNameLength() + getValueLength();
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override public String toString() {
		StringBuilder string_builder = new StringBuilder(75);
		string_builder.append("AttributeHeader [name_length: ");
		string_builder.append(name_length);
		string_builder.append(" | type_name_length: ");
		string_builder.append(type_name_length);
		string_builder.append(" | value_length: ");
		string_builder.append(value_length);
		string_builder.append(']');
		return string_builder.toString();
	}
	
}
