package snowflake.filesystem.attribute;

import j3l.util.ArrayTool;
import j3l.util.Checker;
import j3l.util.TransformValue2;
import snowflake.GlobalString;
import snowflake.StaticMode;
import snowflake.api.IAttributeValue;
import snowflake.filesystem.manager.IDededuplicationProgressDescription;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.09.23_0
 * @author Johannes B. Latzel
 */
public class DededuplicationProgressDescription implements IAttributeValue<IDededuplicationProgressDescription>, 
															IDededuplicationProgressDescription {

	
	/**
	 * <p></p>
	 */
	private final static int BINARY_DATA_LENGTH = 16;
	
	
	/**
	 * <p></p>
	 */
	private final static int CURRENT_INDEX_POINTER_POSITION = 0;
	
	
	/**
	 * <p></p>
	 */
	private final static int BACKUP_FLAKE_IDENTIFICATION_POSITION = 8;
	
	
	/**
	 * <p></p>
	 */
	private final long current_index_pointer;
	
	
	/**
	 * <p></p>
	 */
	private final long backup_flake_identification;
	
	
	/**
	 * <p></p>
	 * 
	 * @param
	 */
	public DededuplicationProgressDescription(long current_index_pointer, long backup_flake_identification) {
		if( StaticMode.TESTING_MODE ) {
			this.current_index_pointer = Checker.checkForBoundaries(
				current_index_pointer, (byte)0, Byte.MAX_VALUE, GlobalString.CurrentIndexPointer.toString()
			);
			this.backup_flake_identification = Checker.checkForBoundaries(
				backup_flake_identification, 0, Long.MAX_VALUE, GlobalString.BackupFlakeIdentification.toString()
			);
		}
		else {
			this.current_index_pointer = current_index_pointer;
			this.backup_flake_identification = backup_flake_identification;
		}
	}
	
	
	/**
	 * <p></p>
	 * 
	 * @param
	 */
	public DededuplicationProgressDescription(byte[] buffer) {
		this(
			TransformValue2.toLong(
				ArrayTool.transferValues(
					new byte[Long.BYTES],
					buffer,
					0,
					DededuplicationProgressDescription.CURRENT_INDEX_POINTER_POSITION,
					Long.BYTES,
					StaticMode.TESTING_MODE
				)
			),
			TransformValue2.toLong(
				ArrayTool.transferValues(
					new byte[Long.BYTES],
					buffer,
					0,
					DededuplicationProgressDescription.BACKUP_FLAKE_IDENTIFICATION_POSITION,
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
			TransformValue2.toByteArray(current_index_pointer, long_buffer),
			DededuplicationProgressDescription.CURRENT_INDEX_POINTER_POSITION,
			0,
			Long.BYTES
		);
		ArrayTool.transferValues(
			buffer,
			TransformValue2.toByteArray(backup_flake_identification, long_buffer),
			DededuplicationProgressDescription.BACKUP_FLAKE_IDENTIFICATION_POSITION,
			0,
			Long.BYTES
		);
	}
	
	
	/* (non-Javadoc)
	 * @see j3l.util.IBinaryData#getDataLength()
	 */
	@Override public int getDataLength() {
		return DededuplicationProgressDescription.BINARY_DATA_LENGTH;
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.filesystem.manager.IDeduplicationDescription#getCurrentIndexPointer()
	 */
	@Override public long getCurrentIndexPointer() {
		return current_index_pointer;
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.filesystem.manager.IDededuplicationProgressDescription#getBackupFlakeIdentification()
	 */
	@Override public long getBackupFlakeIdentification() {
		return backup_flake_identification;
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IAttributeValue#getValue()
	 */
	@Override public IDededuplicationProgressDescription getValue() {
		return this;
	}
	
}
