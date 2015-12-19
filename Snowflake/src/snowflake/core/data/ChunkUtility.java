package snowflake.core.data;

import java.util.zip.CRC32;

import j3l.util.array.ArrayTool;
import j3l.util.check.ArgumentChecker;
import j3l.util.check.ElementChecker;
import j3l.util.transform.TransformValue;
import snowflake.core.flake.Flake;
import snowflake.core.manager.ChunkManager;
import snowflake.core.manager.FlakeManager;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2015.12.14_0
 * @author Johannes B. Latzel
 */
public final class ChunkUtility {
	
	
	/**
	 * <p></p>
	 */
	public final static int BINARY_CHUNK_SIZE = 32;
	
	
	/**
	 * <p></p>
	 */
	private final static int START_ADDRESS_POSITION = 0;
	
	
	/**
	 * <p></p>
	 */
	private final static int LENGTH_POSITION = 8;
	
	
	/**
	 * <p></p>
	 */
	private final static int FLAKE_IDENTIFICATION_POSITION = 16;
	
	
	/**
	 * <p></p>
	 */
	private final static int INDEX_IN_FLAKE_POSITION = 24;
	
	
	/**
	 * <p></p>
	 */
	private final static int CHECKSUM_POSITION = 28;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public static void getBinaryData(ChunkData chunk_data, byte[] buffer) {

		ArgumentChecker.checkForNull(chunk_data, "chunk_data");
		ArgumentChecker.checkForNull(buffer, "buffer");
		
		if( buffer.length != ChunkUtility.BINARY_CHUNK_SIZE ) {
			throw new IllegalArgumentException("The length of the buffer must be equal to " + ChunkUtility.BINARY_CHUNK_SIZE);
		}
		
		
		if( chunk_data == ChunkManager.NULL_CHUNK_DATA ) {
			for(int a=0;a<buffer.length;a++) {
				buffer[a] = 0;
			}
			return;
		}
		
		
		CRC32 checksum = new CRC32();
		byte[] long_buffer = new byte[8];
		byte[] int_buffer = new byte[4];

		
		TransformValue.toByteArray(chunk_data.getStartAddress(), long_buffer);
		checksum.update(long_buffer);
		ArrayTool.transferValues(buffer, long_buffer, ChunkUtility.START_ADDRESS_POSITION);
		
		TransformValue.toByteArray(chunk_data.getChunkLength(), long_buffer);
		checksum.update(long_buffer);
		ArrayTool.transferValues(buffer, long_buffer, ChunkUtility.LENGTH_POSITION);
		
		TransformValue.toByteArray(chunk_data.getFlakeIdentification(), long_buffer);
		checksum.update(long_buffer);
		ArrayTool.transferValues(buffer, long_buffer, ChunkUtility.FLAKE_IDENTIFICATION_POSITION);
		
		TransformValue.toByteArray(chunk_data.getIndexInFlake(), int_buffer);
		checksum.update(int_buffer);
		ArrayTool.transferValues(buffer, int_buffer, ChunkUtility.INDEX_IN_FLAKE_POSITION);
		
		// cast is necessary, because the actual checksum returned is 32 bit integer
		TransformValue.toByteArray((int)checksum.getValue(), int_buffer);
		ArrayTool.transferValues(buffer, int_buffer, ChunkUtility.CHECKSUM_POSITION);
		
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public static ChunkData getChunkData(byte[] buffer) {
		
		ArgumentChecker.checkForNull(buffer, "buffer");
		
		if( buffer.length != ChunkUtility.BINARY_CHUNK_SIZE ) {
			throw new IllegalArgumentException("The length of the buffer must be equal to ChunkUtility.BINARY_CHUNK_SIZE: "
					+ ChunkUtility.BINARY_CHUNK_SIZE + "!");
		}
		
		
		if( ElementChecker.checkAllElementsForZero(buffer) ) {
			throw new SecurityException("All elements of the buffer are equal to 0!");
		}
		
		
		byte[] long_buffer = new byte[8];
		byte[] int_buffer = new byte[4];
		
		CRC32 checksum = new CRC32();
		int read_in_checksum;
		long start_address;
		long length;
		long flake_identification;
		int index_in_flake;
		

		ArrayTool.transferValues(long_buffer, buffer, 0, ChunkUtility.START_ADDRESS_POSITION, long_buffer.length);
		checksum.update(long_buffer);
		start_address = TransformValue.toLong(long_buffer);	
		
		ArrayTool.transferValues(long_buffer, buffer, 0, ChunkUtility.LENGTH_POSITION, long_buffer.length);
		checksum.update(long_buffer);
		length = TransformValue.toLong(long_buffer);	
		
		ArrayTool.transferValues(long_buffer, buffer, 0, ChunkUtility.FLAKE_IDENTIFICATION_POSITION, long_buffer.length);
		checksum.update(long_buffer);
		flake_identification = TransformValue.toLong(long_buffer);		
		
		ArrayTool.transferValues(int_buffer, buffer, 0, ChunkUtility.INDEX_IN_FLAKE_POSITION, int_buffer.length);
		checksum.update(int_buffer);
		index_in_flake = TransformValue.toInteger(int_buffer);	
		
		ArrayTool.transferValues(int_buffer, buffer, 0, ChunkUtility.CHECKSUM_POSITION, int_buffer.length);
		read_in_checksum = TransformValue.toInteger(int_buffer);	
		
		// cast is necessary, because the actual checksum returned is 32 bit integer
		if( (int)checksum.getValue() == read_in_checksum ) {
			return new ChunkData(start_address, length, flake_identification, index_in_flake);
		}
		else {
			throw new SecurityException("The read-in checksum does not match the calculated checksum!");
		}
		
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public static ChunkData getChunkData(Flake owner_flake, Chunk chunk) {

		ArgumentChecker.checkForNull(chunk, "chunk");
		
		if( !chunk.isValid() ) {
			throw new SecurityException("The chunk must be valid!");
			
		}
		
		
		long identification = FlakeManager.ROOT_IDENTIFICATION;
		int index_in_flake = 0;
		
		
		if( owner_flake != null ) {
			identification = owner_flake.getIdentification();
			index_in_flake = owner_flake.getIndexOfChunk(chunk);
			if( index_in_flake < 0 ) {
				throw new IllegalArgumentException("The chunk \"" + chunk.toString() + "\" is not part of the flake\""
						+ owner_flake.toString() + "\"!");
			}
		}
		
		
		return new ChunkData(chunk.getStartAddress(), chunk.getLength(), identification, index_in_flake);
		
	}
	
}
