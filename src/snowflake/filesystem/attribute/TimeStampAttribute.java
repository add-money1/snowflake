package snowflake.filesystem.attribute;

import java.time.Instant;

import j3l.util.TransformValue2;
import snowflake.api.IAttributeValue;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.06.12_0
 * @author Johannes B. Latzel
 */
public final class TimeStampAttribute implements IAttributeValue<Long> {
	
	
	/**
	 * <p></p>
	 */
	private final static int TIME_STAMP_ATTRIBUTE_LENGTH = 8;
	
	
	/**
	 * <p></p>
	 */
	private final long time_stamp;
	
	
	/**
	 * <p></p>
	 * 
	 * @param
	 */
	public TimeStampAttribute(Instant instant) {
		this(instant.toEpochMilli());
	}
	
	
	/**
	 * <p></p>
	 * 
	 * @param
	 */
	public TimeStampAttribute(Long time_stamp) {
		this(time_stamp.longValue());
	}
	
	
	/**
	 * <p></p>
	 * 
	 * @param
	 */
	public TimeStampAttribute(long time_stamp) {
		this.time_stamp = time_stamp;
	}
	
	
	/**
	 * <p></p>
	 * 
	 * @param
	 */
	public TimeStampAttribute(byte[] buffer) {
		this.time_stamp = TransformValue2.toLong(buffer);
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.core.IBinaryData#getBinaryData(byte[])
	 */
	@Override public void getBinaryData(byte[] buffer) {
		TransformValue2.toByteArray(time_stamp, buffer);
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.core.IBinaryData#getDataLength()
	 */
	@Override public int getDataLength() {
		return TimeStampAttribute.TIME_STAMP_ATTRIBUTE_LENGTH;
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.api.IAttributeValue#getValue()
	 */
	@Override public Long getValue() {
		return new Long(time_stamp);
	}
	
}
