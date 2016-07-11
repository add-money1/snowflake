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
public final class FileData extends NodeData {
	
	
	/**
	 * <p></p>
	 */
	public final static int FILE_DATA_LENGTH = 25;
	
	
	/**
	 * <p></p>
	 */
	private final static int ATTRIBUTE_FLAKE_IDENTIFICATION_POSITION = 0;
	
	
	/**
	 * <p></p>
	 */
	private final static int DATA_FLAKE_IDENTIFICATION_POSITION = 8;
	
	
	/**
	 * <p></p>
	 */
	private final static int PARENT_DIRECTORY_IDENTIFICATION_POSITION = 16;
	
	
	/**
	 * <p></p>
	 */
	private final static int FLAG_VECTOR_POSITION = 24;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public static byte[] getBinaryData(byte[] buffer, long attribute_flake_identification,
			long data_flake_identification, long parent_directory_identification, boolean is_empty) {
		Checker.checkForNull(buffer, GlobalString.Buffer.toString());
		int data_length = FileData.FILE_DATA_LENGTH;
		Checker.checkForBoundaries(
			buffer.length, data_length, data_length, GlobalString.BufferLength.toString()
		);
		byte[] long_buffer = new byte[8];
		ArrayTool.transferValues(
			buffer, TransformValue2.toByteArray(attribute_flake_identification, long_buffer),
			FileData.ATTRIBUTE_FLAKE_IDENTIFICATION_POSITION
		);
		ArrayTool.transferValues(
			buffer, TransformValue2.toByteArray(data_flake_identification, long_buffer),
			FileData.DATA_FLAKE_IDENTIFICATION_POSITION
		);
		ArrayTool.transferValues(
			buffer, TransformValue2.toByteArray(parent_directory_identification, long_buffer),
			FileData.PARENT_DIRECTORY_IDENTIFICATION_POSITION
		);
		buffer[FileData.FLAG_VECTOR_POSITION] = (byte)( is_empty ? 1 : 0 );
		return buffer;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public static byte[] createBuffer() {
		return new byte[ FileData.FILE_DATA_LENGTH ];
	}
	
	
	/**
	 * <p></p>
	 */
	private final long attribute_flake_identification;
	
	
	/**
	 * <p></p>
	 */
	private final long data_flake_identification;
	
	
	/**
	 * <p></p>
	 */
	private final long parent_directory_identification;
	
	
	/**
	 * <p></p>
	 */
	private final boolean is_empty;
	
	
	/**
	 * <p></p>
	 * 
	 * @param
	 */
	public FileData(byte[] buffer, long index) {
		super(index);
		Checker.checkForNull(buffer, GlobalString.Buffer.toString());
		int data_length = getDataLength();
		Checker.checkForBoundaries(
			buffer.length, data_length, data_length, GlobalString.BufferLength.toString()
		);
		byte[] long_buffer = new byte[8];
		attribute_flake_identification = TransformValue2.toLong(
			ArrayTool.transferValues(
				long_buffer, buffer, 0, FileData.ATTRIBUTE_FLAKE_IDENTIFICATION_POSITION,
				long_buffer.length
			)
		);
		data_flake_identification = TransformValue2.toLong(
			ArrayTool.transferValues(
				long_buffer, buffer, 0, FileData.DATA_FLAKE_IDENTIFICATION_POSITION,
				long_buffer.length
			)
		);
		parent_directory_identification = TransformValue2.toLong(
			ArrayTool.transferValues(
				long_buffer, buffer, 0, FileData.PARENT_DIRECTORY_IDENTIFICATION_POSITION,
				long_buffer.length
			)
		);
		is_empty = buffer[FileData.FLAG_VECTOR_POSITION] == 1;
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
	public long getDataFlakeIdentification() {
		return data_flake_identification;
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
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public boolean isEmpty() {
		return is_empty;
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.core.data.IBinaryData#getBinaryData(byte[])
	 */
	@Override public void getBinaryData(byte[] buffer) {
		FileData.getBinaryData(
			buffer, attribute_flake_identification, data_flake_identification,
			parent_directory_identification, is_empty
		);
	}
	
	
	/* (non-Javadoc)
	 * @see snowflake.core.data.IBinaryData#getDataLength()
	 */
	@Override public int getDataLength() {
		return FileData.FILE_DATA_LENGTH;
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override public String toString() {
		StringBuilder string_builder = new StringBuilder(110);
		string_builder.append("FileData: [attribute_flake_identification = ");
		string_builder.append(attribute_flake_identification);
		string_builder.append(" | data_flake_identification = ");
		string_builder.append(data_flake_identification);
		string_builder.append(" | parent_directory_identification = ");
		string_builder.append(parent_directory_identification);
		string_builder.append(" | is_empty = ");
		string_builder.append(is_empty);
		string_builder.append("]");
		return string_builder.toString();
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override public boolean equals(Object object) {
		if( object != null && object instanceof FileData ) {
			FileData file_data = (FileData)object;
			if( file_data.hashCode() == hashCode() ) {
				return file_data.attribute_flake_identification == attribute_flake_identification
						&& file_data.data_flake_identification == data_flake_identification
						&& file_data.parent_directory_identification == parent_directory_identification
						&& file_data.is_empty == is_empty;
			}
		}
		return false;
	}
	
}
