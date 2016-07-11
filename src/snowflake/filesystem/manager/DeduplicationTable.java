package snowflake.filesystem.manager;

import java.util.ArrayList;
import java.util.HashMap;

import j3l.util.Checker;
import snowflake.GlobalString;
import snowflake.api.FileSystemException;
import snowflake.api.IFlake;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.07.11_0
 * @author Johannes B. Latzel
 */
public final class DeduplicationTable {
	
	
	/**
	 * <p></p>
	 */
	private final HashMap<byte[], Object> deduplication_map;
	
	
	/**
	 * <p></p>
	 */
	private final IFlake deduplication_table_flake;
	
	
	/**
	 * <p></p>
	 * 
	 * @param
	 */
	public DeduplicationTable(IFlake deduplication_table_flake) {
		this.deduplication_table_flake = Checker.checkForValidation(
			deduplication_table_flake, GlobalString.DeduplicationTableFlake.toString()
		);
		deduplication_map = new HashMap<>();
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void loadDeduplicationBlock(byte[] buffer) {
		//
		buffer[0] = 0;
		this.deduplication_table_flake.delete();
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	@SuppressWarnings("unchecked") public void add(DeduplicationBlock deduplication_block) {
		Checker.checkForNull(deduplication_block, GlobalString.DeduplicationBlock.toString());
		byte[] buffer = deduplication_block.getBlockBuffer();
		final Object object;
		synchronized( deduplication_map ) {
			object = deduplication_map.get(buffer);
		}
		if( object == null ) {
			synchronized( deduplication_map ) {
				deduplication_map.put(buffer, deduplication_block);
			}
		}
		else if( object instanceof DeduplicationBlock || object instanceof ArrayList<?> ) {
			ArrayList<DeduplicationBlock> list;
			if( object instanceof DeduplicationBlock ) {
				if( object.equals(deduplication_block) ) {
					throw new FileSystemException("The deduplication_block " + deduplication_block.toString()
					+ " is already part of the map!");
				}
				list = new ArrayList<>(2);
				list.add((DeduplicationBlock)object);
			}
			else {
				try {
					list = (ArrayList<DeduplicationBlock>)object;
				}
				catch( ClassCastException e ) {
					throw new FileSystemException(
						"The object \"" + object.toString() + "\" stored in the map is not an "
						+ "ArrayList<DeduplicationBlock>!", e
					);
				}
				if( list.contains(deduplication_block) ) {
					throw new FileSystemException("The deduplication_block " + deduplication_block.toString()
					+ " is already part of the map!");
				}
			}
			list.add(deduplication_block);
			synchronized( deduplication_map ) {
				deduplication_map.put(buffer, list);
			}
		}
		else {
			throw new FileSystemException("Can not add the deduplication_block to \"" + object.toString() + "\"!");
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return the index of the associated deplucation_block or -1 if it does not exist
	 */
	@SuppressWarnings("unchecked") public long getIndex(byte[] data_block) {
		Checker.checkForNull(data_block, GlobalString.DataBlock.toString());
		Checker.checkForBoundaries(
			data_block.length,
			DeduplicationBlock.DEDUPLICATION_BLOCK_SIZE,
			DeduplicationBlock.DEDUPLICATION_BLOCK_SIZE,
			GlobalString.DataBlock.toString()
		);
		Object object;
		synchronized( deduplication_map ) {
			object = deduplication_map.get(data_block);
		}
		if( object == null ) {
			return -1;
		}
		if( object instanceof DeduplicationBlock ) {
			return ((DeduplicationBlock)object).getIndex();
		}
		if( object instanceof ArrayList<?> ) {
			ArrayList<DeduplicationBlock> list;
			try {
				list = (ArrayList<DeduplicationBlock>)object;
			}
			catch( ClassCastException e ) {
				throw new FileSystemException(
					"The object \"" + object.toString() + "\" stored in the map is not an "
					+ "ArrayList<DeduplicationBlock>!", e
				);
			}
			for( DeduplicationBlock deduplication_block : list ) {
				if( deduplication_block.hasData(data_block) ) {
					return deduplication_block.getIndex();
				}
			}
			return -1;
		}
		throw new FileSystemException("Can not process the associated entries \"" + object.toString() + "\"!");
	}
	
}
