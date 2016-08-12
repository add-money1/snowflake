package snowflake.api;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

import j3l.util.Checker;
import j3l.util.InputUtility;
import snowflake.GlobalString;
import snowflake.StaticMode;
import snowflake.core.Flake;
import snowflake.core.FlakeInputStream;
import snowflake.filesystem.File;
import snowflake.filesystem.manager.DeduplicationBlock;
import snowflake.filesystem.manager.DeduplicationTable;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.08.12_0
 * @author Johannes B. Latzel
 */
public final class DededuplicationFileInputStream implements Closeable {
	
	
	/**
	 * <p></p>
	 */
	private final ByteBuffer data_buffer;
	
	
	/**
	 * <p></p>
	 */
	private final FlakeInputStream flake_input_stream;
	
	
	/**
	 * <p></p>
	 */
	private final DeduplicationDataPointer deduplication_data_pointer;
	
	
	/**
	 * <p></p>
	 */
	private final DeduplicationTable deduplication_table;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 * @throws IOException 
	 */
	public DededuplicationFileInputStream(File file, Flake data_flake, DeduplicationTable deduplication_table)
			throws IOException {
		if( StaticMode.TESTING_MODE ) {
			Checker.checkForNull(file, GlobalString.File.toString());
			Checker.checkForNull(data_flake, GlobalString.DataFlake.toString());
			Checker.checkForNull(deduplication_table, GlobalString.DeduplicationTable.toString());
			this.deduplication_table = Checker.checkForNull(
				deduplication_table, GlobalString.DeduplicationTable.toString()
			);
		}
		else {
			this.deduplication_table = deduplication_table;
		}
		if( !file.isDeduplicated() ) {
			throw new IOException("The file is not deduplicated!");
		}
		this.deduplication_data_pointer = new DeduplicationDataPointer(file);
		flake_input_stream = data_flake.getFlakeInputStream();
		data_buffer = ByteBuffer.allocate(DeduplicationBlock.SIZE);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 * @throws IOException 
	 */
	private void loadBuffer() throws IOException {
		if( !data_buffer.hasRemaining() ) {
			ByteBuffer long_buffer = ByteBuffer.allocate(Long.BYTES);
			long index_position = deduplication_data_pointer.getDeduplicationIndexPosition();
			flake_input_stream.getDataPointer().setPosition(index_position);
			InputUtility.readComplete(flake_input_stream, long_buffer);
			long_buffer.flip();
			DeduplicationBlock deduplication_block = deduplication_table.getDataBlock(long_buffer.getLong());
			byte[] block_buffer = deduplication_block.getBlockBuffer();
			data_buffer.rewind();
			data_buffer.put(block_buffer);
			data_buffer.flip();
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public int read() throws IOException {
		if( !data_buffer.hasRemaining() ) {
			loadBuffer();
		}
		return data_buffer.get();
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public int read(byte[] buffer, int offset, int length) throws IOException {
		int remaining_bytes = length;
		int current_bytes;
		while( remaining_bytes > 0 ) {
			if( !data_buffer.hasRemaining() ) {
				loadBuffer();
			}
			current_bytes = data_buffer.remaining();
			data_buffer.get(buffer, offset + remaining_bytes - length, remaining_bytes);
			if( remaining_bytes > current_bytes ) {
				remaining_bytes -= current_bytes;
			}
			else {
				remaining_bytes = 0;
			}
		}
		return length;
	}
	
	
	/*
	 * (non-Javadoc)
	 * @see java.io.Closeable#close()
	 */
	@Override public synchronized void close() throws IOException {
		flake_input_stream.close();
	}
	
}
