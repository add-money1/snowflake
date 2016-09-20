package snowflake.filesystem.manager;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;

import j3l.util.Checker;
import snowflake.GlobalString;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.07.21_0
 * @author Johannes B. Latzel
 */
public final class DeduplicationBlock {
	
	
	/**
	 * <p></p>
	 */
	public final static int SIZE = Long.BYTES * 512;
	
	
	/**
	 * <p></p>
	 */
	private ByteBuffer block_buffer;
	
	
	/**
	 * <p></p>
	 */
	private final long index;
	
	
	/**
	 * <p></p>
	 */
	private long last_used_time;
	
	
	/**
	 * <p></p>
	 */
	private DeduplicationTable deduplication_table;
	
	
	/**
	 * <p></p>
	 * 
	 * @param
	 */
	public DeduplicationBlock(DeduplicationTable deduplication_table, long index) {
		this.deduplication_table = Checker.checkForNull(
			deduplication_table, GlobalString.DeduplicationTable.toString()
		);
		this.index = Checker.checkForBoundaries(
			index, 0, Long.MAX_VALUE, GlobalString.Index.toString()
		);
		block_buffer = null;
		last_used_time = Instant.now().toEpochMilli();
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private void load() {
		if( block_buffer == null ) {
			block_buffer = ByteBuffer.allocate(DeduplicationBlock.SIZE);
		}
		deduplication_table.loadDeduplicationBlock(this, block_buffer);
		block_buffer.rewind();
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private ByteBuffer getBlockBufferInternal() {
		if( block_buffer == null ) {
			load();
		}
		last_used_time = Instant.now().toEpochMilli();
		return block_buffer;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public ByteBuffer getBlockBuffer() {
		byte[] internal = getBlockBufferInternal().array();
		return ByteBuffer.wrap(Arrays.copyOf(internal, internal.length));
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void clearWhenUnusedFor(long amount_of_milli_seconds) {
		if( block_buffer != null && Instant.now().toEpochMilli() - last_used_time >= amount_of_milli_seconds ) {
			block_buffer = null;
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public long getIndex() {
		return index;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public boolean hasData(ByteBuffer data_block) {
		Checker.checkForNull(data_block, GlobalString.DataBlock.toString()).rewind();
		ByteBuffer block_buffer = getBlockBufferInternal();
		return Checker.checkAllElementsForEquality(block_buffer, data_block);
	}

}
