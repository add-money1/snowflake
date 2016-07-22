package snowflake.filesystem.attribute;

import j3l.util.ArrayTool;
import j3l.util.Checker;
import j3l.util.TransformValue2;
import snowflake.GlobalString;
import snowflake.StaticMode;
import snowflake.api.IAttributeValue;
import snowflake.filesystem.manager.IDeduplicationProgressDescription;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.07.22_0
 * @author Johannes B. Latzel
 */
public class DeduplicationProgressDescription implements IAttributeValue<IDeduplicationProgressDescription>, IDeduplicationProgressDescription {
	
	
	/**
	 * <p></p>
	 */
	private final static int BINARY_DATA_LENGTH = 16;
	
	
	/**
	 * <p></p>
	 */
	private final static int CURRENT_DATA_POINTER_POSITION = 0;
	
	
	/**
	 * <p></p>
	 */
	private final static int CURRENT_INDEX_POINTER_POSITION = 8; 
	
	
	/**
	 * <p></p>
	 */
	private final long current_data_pointer;
	
	
	/**
	 * <p></p>
	 */
	private final long current_index_pointer;
	
	
	/**
	 * <p></p>
	 * 
	 * @param
	 */
	public DeduplicationProgressDescription(long current_data_pointer, long current_index_pointer) {
		if( StaticMode.TESTING_MODE ) {
			this.current_data_pointer = Checker.checkForBoundaries(
					current_data_pointer, (byte)0, Byte.MAX_VALUE, GlobalString.CurrentDataPointer.toString()
			);
			this.current_index_pointer = Checker.checkForBoundaries(
				current_index_pointer, 0, Long.MAX_VALUE, GlobalString.CurrentIndexPointer.toString()
			);
		}
		else {
			this.current_data_pointer = current_data_pointer;
			this.current_index_pointer = current_index_pointer;
		}
	}
	
	
	/**
	 * <p></p>
	 * 
	 * @param
	 */
	public DeduplicationProgressDescription(byte[] buffer) {
		this(
			TransformValue2.toLong(
				ArrayTool.transferValues(
					new byte[Long.BYTES],
					buffer,
					0,
					DeduplicationProgressDescription.CURRENT_DATA_POINTER_POSITION,
					Long.BYTES,
					StaticMode.TESTING_MODE
				)
			),
			TransformValue2.toLong(
				ArrayTool.transferValues(
					new byte[Long.BYTES],
					buffer,
					0,
					DeduplicationProgressDescription.CURRENT_INDEX_POINTER_POSITION,
					Long.BYTES,
					StaticMode.TESTING_MODE
				)
			)
		);
	}
	
	
	/* (non-Javadoc)
	 * @see j3l.util.IBinaryData#getBinaryData(byte[])
	 */
	@Override public void getBinaryData(byte[] buffer) {
		byte[] long_buffer = new byte[Long.BYTES];
		ArrayTool.transferValues(
			buffer,
			TransformValue2.toByteArray(current_data_pointer, long_buffer),
			DeduplicationProgressDescription.CURRENT_DATA_POINTER_POSITION,
			0,
			Long.BYTES
		);
		ArrayTool.transferValues(
			buffer,
			TransformValue2.toByteArray(current_index_pointer, long_buffer),
			DeduplicationProgressDescription.CURRENT_INDEX_POINTER_POSITION,
			0,
			Long.BYTES
		);
	}
	
	
	/* (non-Javadoc)
	 * @see j3l.util.IBinaryData#getDataLength()
	 */
	@Override public int getDataLength() {
		return DeduplicationProgressDescription.BINARY_DATA_LENGTH;
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.filesystem.manager.IDeduplicationDescription#getCurrentDataPointer()
	 */
	@Override public long getCurrentDataPointer() {
		return current_data_pointer;
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.filesystem.manager.IDeduplicationDescription#getCurrentIndexPointer()
	 */
	@Override public long getCurrentIndexPointer() {
		return current_index_pointer;
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IAttributeValue#getValue()
	 */
	@Override public IDeduplicationProgressDescription getValue() {
		return this;
	}
	
}
