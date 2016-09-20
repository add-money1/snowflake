package snowflake.filesystem.manager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;

import j3l.util.Checker;
import snowflake.GlobalString;
import snowflake.StaticMode;
import snowflake.api.FileSystemException;
import snowflake.api.IFlake;
import snowflake.core.FlakeInputStream;
import snowflake.core.FlakeOutputStream;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.09.21_0
 * @author Johannes B. Latzel
 */
public final class DeduplicationTable {
	
	
	/**
	 * <p></p>
	 */
	private static void checkDataBlock(ByteBuffer data_block) {
		if( StaticMode.TESTING_MODE ) {
			Checker.checkForNull(data_block, GlobalString.Buffer.toString());
		}
		data_block.rewind();
		if( data_block.remaining() != DeduplicationBlock.SIZE ) {
			throw new FileSystemException(
				"The buffer.length must be equal to DeduplicationBlock.SIZE " + DeduplicationBlock.SIZE + ", but is "
				+ data_block.capacity()
			);
		}
	}
	
	
	/**
	 * <p></p>
	 */
	private final HashMap<ByteBuffer, Object> deduplication_map;
	
	
	/**
	 * <p></p>
	 */
	private final FlakeOutputStream table_output;
	
	
	/**
	 * <p></p>
	 */
	private final FlakeInputStream table_input;
	
	
	/**
	 * <p></p>
	 */
	private final IFlake deduplication_table_flake;
	
	
	/**
	 * <p></p>
	 * 
	 * @param
	 * @throws IOException 
	 */
	public DeduplicationTable(IFlake deduplication_table_flake) throws IOException {
		this.deduplication_table_flake = Checker.checkForValidation(
			deduplication_table_flake, GlobalString.DeduplicationTableFlake.toString()
		);
		this.table_output = deduplication_table_flake.getFlakeOutputStream();
		this.table_input = deduplication_table_flake.getFlakeInputStream();
		deduplication_map = new HashMap<>();
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public ByteBuffer loadDeduplicationBlock(DeduplicationBlock deduplication_block, ByteBuffer buffer) {
		DeduplicationTable.checkDataBlock(buffer);
		synchronized( deduplication_table_flake ) {
			synchronized( table_input ) {
				table_input.getDataPointer().setPosition(
					DeduplicationBlock.SIZE * deduplication_block.getIndex()
				);
				try {
					table_input.read(buffer);
				}
				catch( IOException e ) {
					throw new FileSystemException(
						"Can not read in the block with index: " + deduplication_block.getIndex() + "!", e
					);
				}
			}
		}
		return buffer;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	@SuppressWarnings("unchecked") public long register(ByteBuffer data_block) {
		DeduplicationTable.checkDataBlock(data_block);
		if( getIndex(data_block) != -1 ) {
			throw new FileSystemException("The block is already registered!");
		}
		long index;
		synchronized( deduplication_table_flake ) {
			if( StaticMode.TESTING_MODE ) {
				if( deduplication_table_flake.getLength() % DeduplicationBlock.SIZE != 0 ) {
					throw new FileSystemException("The deduplication_table_flake has an invalid length: "
					+ "deduplication_table_flake.getLength() % DeduplicationBlock.SIZE == "
					+ (deduplication_table_flake.getLength() % DeduplicationBlock.SIZE));
				}
			}
			index = deduplication_table_flake.getLength() / DeduplicationBlock.SIZE;
			synchronized( table_output ) {
				table_output.getDataPointer().seekEOF();
				deduplication_table_flake.expandAtEnd(DeduplicationBlock.SIZE);
				try {
					table_output.write(data_block);
				}
				catch( IOException e ) {
					// remove the previously added space
					deduplication_table_flake.cutFromEnd(DeduplicationBlock.SIZE);
					throw new FileSystemException("Could not register the block of data!", e);
				}
			}
		}
		final DeduplicationBlock deduplication_block = new DeduplicationBlock(this,  index);
		final Object object;
		synchronized( deduplication_map ) {
			object = deduplication_map.get(data_block);
		}
		if( object == null ) {
			synchronized( deduplication_map ) {
				deduplication_map.put(data_block, deduplication_block);
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
				deduplication_map.put(data_block, list);
			}
		}
		else {
			throw new FileSystemException("Can not add the deduplication_block to \"" + object.toString() + "\"!");
		}
		return index;
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return the index of the associated deplucation_block or -1 if it does not exist
	 */
	@SuppressWarnings("unchecked") public long getIndex(ByteBuffer data_block) {
		Checker.checkForNull(data_block, GlobalString.DataBlock.toString());
		Checker.checkForBoundaries(
			data_block.capacity(),
			DeduplicationBlock.SIZE,
			DeduplicationBlock.SIZE,
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
	
	
	/**
	 * @param index
	 */
	@SuppressWarnings("unchecked") public DeduplicationBlock getDataBlock(long index) {
		DeduplicationBlock current_block;
		ArrayList<DeduplicationBlock> current_list;
		synchronized( deduplication_map ) {
			for( Object o : deduplication_map.values() ) {
				if( o instanceof DeduplicationBlock ) {
					current_block = (DeduplicationBlock)o;
					if( current_block.getIndex() == index ) {
						return current_block;
					}
				}
				else if( o instanceof ArrayList<?> ) {
					current_list = (ArrayList<DeduplicationBlock>)o;
					for( DeduplicationBlock block : current_list ) {
						if( block.getIndex() == index ) {
							return block;
						}
					}
				}
			}
		}
		throw new FileSystemException("The deduplication_block with the index " + index + " does not exist!");
	}
	
}
