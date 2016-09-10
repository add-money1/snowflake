package snowflake.filesystem.attribute;

import j3l.util.ArrayTool;
import j3l.util.Checker;
import j3l.util.TransformValue2;
import snowflake.GlobalString;
import snowflake.StaticMode;
import snowflake.filesystem.manager.IDeduplicationDescription;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.09.10_0
 * @author Johannes B. Latzel
 */
public final class DeduplicationDescription implements IDeduplicationDescription {
	
	
	/**
	 * <p></p>
	 */
	private final static int BINARY_DATA_LENGTH = 8;
	
	
	/**
	 * <p></p>
	 */
	private final static int END_OF_DEDUPLICATION_POINTER_POSITION = 0;
	
	
	/**
	 * <p></p>
	 */
	private final static long getEndOfDeduplicationPointer(byte[] buffer) {
		if( StaticMode.TESTING_MODE ) {
			Checker.checkForNull(buffer, GlobalString.Buffer.toString());
		}
		int data_length = DeduplicationDescription.BINARY_DATA_LENGTH;
		if( buffer.length % data_length != 0 ) {
			throw new IllegalArgumentException(
				"The length of the buffer must be equal to k * DeduplicationDescription.BINARY_DATA_LENGTH: "
				+ data_length
			);
		}
		return TransformValue2.toLong(
			ArrayTool.transferValues(
				new byte[Long.BYTES],
				buffer,
				0,
				DeduplicationDescription.END_OF_DEDUPLICATION_POINTER_POSITION,
				Long.BYTES,
				StaticMode.TESTING_MODE
			)
		);
	}
	
	
	/**
	 * <p></p>
	 */
	private final long end_of_deduplication_pointer;
	
	
	/**
	 * <p></p>
	 * 
	 * @param
	 */
	public DeduplicationDescription(byte[] buffer) {		
		this(DeduplicationDescription.getEndOfDeduplicationPointer(buffer));
	}
	
	
	/**
	 * <p></p>
	 * 
	 * @param
	 */
	public DeduplicationDescription(long end_of_deduplication_pointer) {
		if( StaticMode.TESTING_MODE ) {
			this.end_of_deduplication_pointer = Checker.checkForBoundaries(
				end_of_deduplication_pointer, 0, Long.MAX_VALUE, GlobalString.EndOfDeduplicationPointer.toString()
			);
		}
		else {
			this.end_of_deduplication_pointer = end_of_deduplication_pointer;
		}
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
		if( StaticMode.TESTING_MODE ) {
			Checker.checkForNull(buffer, GlobalString.Buffer.toString());
			Checker.checkForBoundaries(
				buffer.length, getDataLength(), getDataLength(), GlobalString.BufferLength.toString()
			);
		}
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
	
}
