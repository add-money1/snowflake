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
 * @version 2016.07.22_0
 * @author Johannes B. Latzel
 */
public final class DeduplicationDescription implements IAttributeValue<IDeduplicationDescription>, IDeduplicationDescription {
	
	
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
	private final long end_of_deduplication_pointer;
	
	
	/**
	 * <p></p>
	 */
	private final DeduplicationDescription previous_deduplication_description;
	
	
	/**
	 * <p></p>
	 * 
	 * @param
	 */
	public DeduplicationDescription(DeduplicationDescription previous_deduplication_description,
			long end_of_deduplication_pointer) {
		if( StaticMode.TESTING_MODE ) {
			this.previous_deduplication_description = Checker.checkForNull(
				previous_deduplication_description,
				GlobalString.PreviousDeduplcationDescription.toString()
			);
			this.end_of_deduplication_pointer = Checker.checkForBoundaries(
				end_of_deduplication_pointer, 0, Long.MAX_VALUE, GlobalString.EndOfDeduplicationPointer.toString()
			);
		}
		else {
			this.previous_deduplication_description = previous_deduplication_description;
			this.end_of_deduplication_pointer = end_of_deduplication_pointer;
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
	 * @see snowflake.filesystem.manager.IDeduplicationDescription#isDeduplicated()
	 */
	@Override public boolean isDeduplicated() {
		return deduplication_level > 0;
	}
	
}
