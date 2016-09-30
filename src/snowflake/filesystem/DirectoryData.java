package snowflake.filesystem;

import java.nio.ByteBuffer;

import j3l.util.Checker;
import snowflake.GlobalString;
import snowflake.StaticMode;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.09.30_0
 * @author Johannes B. Latzel
 */
public final class DirectoryData extends NodeData {
	
	
	/**
	 * <p></p>
	 */
	public final static int DIRECTORY_DATA_LENGTH = 16;
	
	
	/**
	 * <p></p>
	 */
	private final static int ATTRIBUTE_FLAKE_IDENTIFICATION_POSITION = 0;
	
	
	/**
	 * <p></p>
	 */
	private final static int PARENT_DIRECTORY_IDENTIFICATION_POSITION = 8;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public static ByteBuffer getBinaryData(ByteBuffer buffer, long attribute_flake_identification,
			long parent_directory_identification) {
		if( StaticMode.TESTING_MODE ) {
			Checker.checkForNull(buffer, GlobalString.Buffer.toString());
		}
		Checker.checkForBoundaries(
			buffer.remaining(),
			DirectoryData.DIRECTORY_DATA_LENGTH,
			DirectoryData.DIRECTORY_DATA_LENGTH,
			GlobalString.BufferLength.toString()
		);
		buffer.putLong(DirectoryData.ATTRIBUTE_FLAKE_IDENTIFICATION_POSITION, attribute_flake_identification);
		buffer.putLong(DirectoryData.PARENT_DIRECTORY_IDENTIFICATION_POSITION, parent_directory_identification);
		return buffer;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public static ByteBuffer createBuffer() {
		return ByteBuffer.allocate(DirectoryData.DIRECTORY_DATA_LENGTH);
	}
	
	
	/**
	 * <p></p>
	 */
	private final long attribute_flake_identification;
	
	
	/**
	 * <p></p>
	 */
	private final long parent_directory_identification;
	
	
	/**
	 * <p></p>
	 * 
	 * @param
	 */
	public DirectoryData(ByteBuffer buffer, long index) {
		super(index);
		if( StaticMode.TESTING_MODE ) {
			Checker.checkForNull(buffer, GlobalString.Buffer.toString());
		}
		Checker.checkForBoundaries(
			buffer.remaining(), getDataLength(), Integer.MAX_VALUE, GlobalString.BufferLength.toString()
		);
		attribute_flake_identification = buffer.getLong();
		parent_directory_identification = buffer.getLong();
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public long getAttributeFlakeIdentification() {
		return attribute_flake_identification;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public long getParentDirectoryIdentification() {
		return parent_directory_identification;
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.core.data.IBinaryData#getBinaryData(byte[])
	 */
	@Override public void getBinaryData(byte[] buffer) {
		DirectoryData.getBinaryData(
			ByteBuffer.wrap(buffer), attribute_flake_identification, parent_directory_identification
		);
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.core.data.IBinaryData#getDataLength()
	 */
	@Override public int getDataLength() {
		return DirectoryData.DIRECTORY_DATA_LENGTH;
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override public String toString() {
		StringBuilder string_builder = new StringBuilder(70);
		string_builder.append("DirectoryData: [attribute_flake_identification = ");
		string_builder.append(attribute_flake_identification);
		string_builder.append(" | parent_directory_identification = ");
		string_builder.append(parent_directory_identification);
		string_builder.append("]");
		return string_builder.toString();
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override public boolean equals(Object object) {
		if( object != null && object instanceof DirectoryData ) {
			DirectoryData file_data = (DirectoryData)object;
			if( file_data.hashCode() == hashCode() ) {
				return file_data.attribute_flake_identification == attribute_flake_identification
						&& file_data.parent_directory_identification == parent_directory_identification;
			}
		}
		return false;
	}
	
}
