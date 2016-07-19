package snowflake.filesystem.attribute;

import j3l.util.ArrayTool;
import j3l.util.Checker;
import j3l.util.TransformValue2;
import snowflake.GlobalString;
import snowflake.StaticMode;
import snowflake.api.IAttributeValue;
import snowflake.filesystem.manager.IDeduplicationDescription;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.07.19_0
 * @author Johannes B. Latzel
 */
public final class DeduplicationDescription implements IAttributeValue<IDeduplicationDescription>, IDeduplicationDescription {
	
	
	/**
	 * <p></p>
	 */
	public final static int BINARY_DATA_LENGTH = 17;
	
	/**
	 * <p></p>
	 */
	public final static int DEDUPLICATION_LEVEL_POSITION = 0;
	
	/**
	 * <p></p>
	 */
	public final static int END_OF_DEDUPLICATION_POINTER_POSITION = 1;
	
	
	/**
	 * <p></p>
	 */
	public final static int END_OF_FILE_POINTER_POSITION = 9;
	
	
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
	 */
	private final long eof_pointer;
	
	
	/**
	 * <p></p>
	 * 
	 * @param
	 */
	public DeduplicationDescription(byte deduplication_level, long end_of_deduplication_pointer, long eof_pointer) {
		if( StaticMode.TESTING_MODE ) {
			this.deduplication_level = Checker.checkForBoundaries(
				deduplication_level, (byte)0, Byte.MAX_VALUE, GlobalString.DeduplicationLevel.toString()
			);
			this.end_of_deduplication_pointer = Checker.checkForBoundaries(
				end_of_deduplication_pointer, 0, Long.MAX_VALUE, GlobalString.EndOfDeduplicationPointer.toString()
			);
			this.eof_pointer = Checker.checkForBoundaries(
				eof_pointer, end_of_deduplication_pointer, Long.MAX_VALUE, GlobalString.EOFPointer.toString()
			);
		}
		else {
			this.deduplication_level = deduplication_level;
			this.end_of_deduplication_pointer = end_of_deduplication_pointer;
			this.eof_pointer = eof_pointer;
		}
	}
	
	
	/**
	 * <p></p>
	 * 
	 * @param
	 */
	public DeduplicationDescription(byte[] buffer) {
		this(
			buffer[DeduplicationDescription.DEDUPLICATION_LEVEL_POSITION],
			TransformValue2.toLong(
				ArrayTool.transferValues(
					new byte[Long.BYTES],
					buffer,
					0,
					DeduplicationDescription.END_OF_DEDUPLICATION_POINTER_POSITION,
					Long.BYTES,
					StaticMode.TESTING_MODE
				)
			),
			TransformValue2.toLong(
				ArrayTool.transferValues(
					new byte[Long.BYTES],
					buffer,
					0,
					DeduplicationDescription.END_OF_FILE_POINTER_POSITION,
					Long.BYTES,
					StaticMode.TESTING_MODE
				)
			)
		);
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see snowflake.filesystem.attribute.IDeduplicationDescription#getDeduplicationLevel()
	 */
	@Override public byte getDeduplicationLevel() {
		return deduplication_level;
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see snowflake.filesystem.attribute.IDeduplicationDescription#getEndOfDeduplicationPointer()
	 */
	@Override public long getEndOfDeduplicationPointer() {
		return end_of_deduplication_pointer;
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.core.IBinaryData#getBinaryData(byte[])
	 */
	@Override public void getBinaryData(byte[] buffer) {
		buffer[DeduplicationDescription.DEDUPLICATION_LEVEL_POSITION] = deduplication_level;
		ArrayTool.transferValues(
			buffer,
			TransformValue2.toByteArray(end_of_deduplication_pointer),
			DeduplicationDescription.END_OF_DEDUPLICATION_POINTER_POSITION,
			0,
			Long.BYTES
		);
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.core.IBinaryData#getDataLength()
	 */
	@Override public int getDataLength() {
		return DeduplicationDescription.BINARY_DATA_LENGTH;
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IAttributeValue#getValue()
	 */
	@Override public IDeduplicationDescription getValue() {
		return this;
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.filesystem.manager.IDeduplicationDescription#getEOFPointer()
	 */
	@Override public long getEOFPointer() {
		return eof_pointer;
	}
	
}
