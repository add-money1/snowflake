package snowflake.filesystem.manager;

import java.time.Instant;
import java.util.Arrays;

import j3l.util.check.ArgumentChecker;
import j3l.util.check.ElementChecker;
import snowflake.GlobalString;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.07.01_0
 * @author Johannes B. Latzel
 */
public final class DeduplicationBlock {
	
	
	/**
	 * <p></p>
	 */
	public final static int DEDUPLICATION_BLOCK_SIZE = 4_096;
	
	
	/**
	 * <p></p>
	 */
	private byte[] block_buffer;
	
	
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
		this.deduplication_table = ArgumentChecker.checkForNull(
				deduplication_table, GlobalString.DeduplicationTable.toString()
		);
		this.index = ArgumentChecker.checkForBoundaries(
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
			block_buffer = new byte[DeduplicationBlock.DEDUPLICATION_BLOCK_SIZE];
		}
		deduplication_table.loadDeduplicationBlock(block_buffer);
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	private byte[] getBlockBufferInternal() {
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
	public byte[] getBlockBuffer() {
		return Arrays.copyOf(getBlockBufferInternal(), block_buffer.length);
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
	public boolean hasData(byte[] data_block) {
		ArgumentChecker.checkForNull(data_block, GlobalString.DataBlock.toString());
		byte[] block_buffer = getBlockBufferInternal();
		return ElementChecker.checkAllElementsForEquality(block_buffer, data_block);
	}

}
