package snowflake.api;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.TreeSet;

import j3l.util.check.ArgumentChecker;
import j3l.util.check.ElementChecker;
import j3l.util.stream.StreamFilter;
import snowflake.core.ChunkUtility;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.03.14_0
 * @author Johannes B. Latzel
 */
public final class DataTable<T extends IBinaryData> {
	
	
	/**
	 * <p></p>
	 */
	private final RandomAccessFile table_file;
	
	
	/**
	 * <p>a list of all available indices</p>
	 */
	private final TreeSet<Long> available_index_tree;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public DataTable(File table_file) {
		ArgumentChecker.checkForExistence(table_file, GlobalString.TableFile.toString());
		try {
			this.table_file = new RandomAccessFile(table_file, "rw");
		}
		catch( FileNotFoundException e ) {
			throw new StorageException("The " + GlobalString.TableFile.toString() + " could not be found!", e);
		}
		available_index_tree = new TreeSet<>();
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 * @throws IOException 
	 */
	public void addEntry(TableMember<T> table_member) {
		if( table_member == null ) {
			return;
		}
		IBinaryData data = table_member.getBinaryData();
		byte[] buffer = new byte[data.getDataLength()];
		try {
			data.getBinaryData(buffer);
			synchronized( table_file ) {
				table_file.seek( buffer.length * table_member.getTableIndex() );
				table_file.write(buffer);
			}
		}
		catch( IOException e ) {
			throw new StorageException("Could not flush the data in this table! Stopped at table_member \""
					+ table_member.toString() + "\".", e);
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public long getAvailableIndex() {
		synchronized( available_index_tree ) {
			if( available_index_tree.size() != 0 ) {
				return available_index_tree.pollFirst().longValue();
			}
			else {
				long length;
				synchronized( table_file ) {
					try {
						length = table_file.length();
					}
					catch( IOException e ) {
						throw new StorageException("No more indices available!", e);
					}
					long current_index = length / ChunkUtility.BINARY_CHUNK_SIZE;
					try {
						table_file.setLength((current_index + 1) * ChunkUtility.BINARY_CHUNK_SIZE);
					} catch (IOException e) {
						throw new StorageException("No more indices available!", e);
					}
					return current_index;
				}
			}
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void addAvailableIndex(long index) {
		Long boxed_index = new Long(index);
		synchronized( available_index_tree ) {
			if( available_index_tree.contains(boxed_index) ) {
				throw new Error("An index must never exist more than once!");
			}
			available_index_tree.add(boxed_index);
		}
	}


	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public void addAvailableIndices(ArrayList<Long> available_index_list) {
		synchronized( available_index_tree ) {
			available_index_list.parallelStream().filter(StreamFilter::filterNull).forEach(index -> {
				if( available_index_tree.contains(index) ) {
					throw new Error("An index must never exist more than once!");
				}
				available_index_tree.add(index);
			});
		}
	}
	
	
	/**
	 * <p>cuts off all empty-entries at the end of the table</p>
	 */
	public void trim() {
		byte[] chunk_buffer = new byte[ChunkUtility.BINARY_CHUNK_SIZE];
		boolean found_non_empty_entry = false;
		long pointer;
		synchronized( available_index_tree) {
			synchronized( table_file ) {
				try {
					pointer = table_file.length() - ChunkUtility.BINARY_CHUNK_SIZE;
					while( pointer >= 0 ) {
						table_file.seek(pointer);
						table_file.readFully(chunk_buffer);
						if( !ElementChecker.checkAllElementsForZero(chunk_buffer) ) {
							found_non_empty_entry = true;
							break;
						}
						pointer -= ChunkUtility.BINARY_CHUNK_SIZE;
					}
					if( found_non_empty_entry ) {
						if( table_file.length() - pointer >= ChunkUtility.BINARY_CHUNK_SIZE ) {
							table_file.setLength(pointer + ChunkUtility.BINARY_CHUNK_SIZE);
						}
					}
					else {
						table_file.setLength(0);
						available_index_tree.clear();
					}
				}
				catch( IOException e ) {
					throw new StorageException("Can not cut the table file.", e);
				}
				// needed, because removeIf wants a final value..
				final long pointer_copy = pointer;
				if( !available_index_tree.isEmpty() ) {
					available_index_tree.removeIf(l -> l.longValue() > pointer_copy);
				}
			}
		}
	}
}
