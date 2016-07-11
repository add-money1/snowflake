package snowflake.filesystem;

import j3l.util.ArrayTool;
import j3l.util.TransformValue2;
import j3l.util.Checker;
import snowflake.GlobalString;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.07.11_0
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
	public static byte[] getBinaryData(byte[] buffer, long attribute_flake_identification,
			long parent_directory_identification) {
		Checker.checkForNull(buffer, GlobalString.Buffer.toString());
		int data_length = DirectoryData.DIRECTORY_DATA_LENGTH;
		Checker.checkForBoundaries(
			buffer.length, data_length, data_length, GlobalString.BufferLength.toString()
		);
		byte[] long_buffer = new byte[8];
		ArrayTool.transferValues(
			buffer, TransformValue2.toByteArray(attribute_flake_identification, long_buffer),
			DirectoryData.ATTRIBUTE_FLAKE_IDENTIFICATION_POSITION
		);
		ArrayTool.transferValues(
			buffer, TransformValue2.toByteArray(parent_directory_identification, long_buffer),
			DirectoryData.PARENT_DIRECTORY_IDENTIFICATION_POSITION
		);
		return buffer;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public static byte[] createBuffer() {
		return new byte[ DirectoryData.DIRECTORY_DATA_LENGTH ];
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
	public DirectoryData(byte[] buffer, long index) {
		super(index);
		Checker.checkForNull(buffer, GlobalString.Buffer.toString());
		int data_length = getDataLength();
		Checker.checkForBoundaries(
			buffer.length, data_length, data_length, GlobalString.BufferLength.toString()
		);
		byte[] long_buffer = new byte[8];
		attribute_flake_identification = TransformValue2.toLong(
			ArrayTool.transferValues(
				long_buffer, buffer, 0, DirectoryData.ATTRIBUTE_FLAKE_IDENTIFICATION_POSITION,
				long_buffer.length
			)
		);
		parent_directory_identification = TransformValue2.toLong(
			ArrayTool.transferValues(
				long_buffer, buffer, 0, DirectoryData.PARENT_DIRECTORY_IDENTIFICATION_POSITION,
				long_buffer.length
			)
		);
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
			buffer, attribute_flake_identification, parent_directory_identification
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
