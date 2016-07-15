package snowflake.filesystem;

import java.io.IOException;
import java.util.ArrayList;

import j3l.util.Checker;
import j3l.util.InputUtility;
import j3l.util.LongRange;
import snowflake.GlobalString;
import snowflake.api.DataPointer;
import snowflake.api.IFlake;
import snowflake.api.StorageException;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.07.11_0
 * @author Johannes B. Latzel
 */
public final class DirectoryTable extends FileSystemDataTable<Directory, DirectoryData> {
	
	
	/**
	 * <p></p>
	 * 
	 * @param
	 */
	public DirectoryTable(IFlake table_flake) throws IOException {
		super(table_flake, DirectoryData.DIRECTORY_DATA_LENGTH);
	}
	
	
	/* (non-Javadoc)
	 * @see j3l.util.DataTable#saveEntry(j3l.util.Indexable)
	 */
	@Override public void saveEntry(Directory directory) throws IOException {
		Checker.checkForValidation(directory, GlobalString.Directory.toString());
		long index = directory.getIndex();
		synchronized( flake_output_stream ) {
			long position = index * DirectoryData.DIRECTORY_DATA_LENGTH;
			if( position < 0 ) {
				throw new StorageException("The index " + index + " is too big!");
			}
			flake_output_stream.getDataPointer().setPosition(position);
			flake_output_stream.write(DirectoryData.getBinaryData(
				DirectoryData.createBuffer(),
				directory.getAttributeFlakeIdentification(),
				directory.getParentDirectory().getIdentification()
			));
		}
	}
	
	
	/* (non-Javadoc)
	 * @see j3l.util.DataTable#deleteEntry(j3l.util.Indexable)
	 */
	@Override public void deleteEntry(Directory directory) throws IOException {
		Checker.checkForValidation(directory, GlobalString.Directory.toString());
		long index = directory.getIndex();
		synchronized( flake_output_stream ) {
			long position = index * DirectoryData.DIRECTORY_DATA_LENGTH;
			if( position < 0 ) {
				throw new StorageException("The index " + index + " is too big!");
			}
			flake_output_stream.getDataPointer().setPosition(position);
			flake_output_stream.write(clear_array);
		}
		synchronized( available_index_list ) {
			available_index_list.sort(LongRange.BY_BEGIN_COMPARATOR);
			for( LongRange range : available_index_list ) {
				if( range.contains(index) ) {
					throw new IOException(
						"Can not add the index to the available_index_list because it already contains it!"
					);
				}
				if( range.elementIsAddable(index) ) {
					range.add(index);
					return;
				}
			}
			available_index_list.add(new LongRange(index, index));
		}
	}
	
	
	/* (non-Javadoc)
	 * @see j3l.util.DataTable#getAllEntries()
	 */
	@Override public ArrayList<DirectoryData> getAllEntries() throws IOException {
		synchronized( flake_input_stream ) {
			DataPointer pointer = flake_input_stream.getDataPointer();
			pointer.setPosition(0);
			long remaining_bytes = pointer.getRemainingBytes();
			int file_data_length = DirectoryData.DIRECTORY_DATA_LENGTH;
			if( remaining_bytes % file_data_length != 0 ) {
				throw new IOException("The file_data has an invalid length: " + remaining_bytes);
			}
			long number_of_entries = remaining_bytes / file_data_length;
			ArrayList<DirectoryData> list = new ArrayList<>(
				// cast is okay, because number_of_entries is smaller than equals Integer.MAX_VALUE
				number_of_entries > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int)number_of_entries
			);
			byte[] buffer = DirectoryData.createBuffer();
			long current_index = 0;
			while( !pointer.isEOF() ) {
				if( !Checker.checkAllElements(InputUtility.readComplete(flake_input_stream, buffer), (byte)0) ) {
					list.add(new DirectoryData(buffer, current_index));
				}
				else {
					addAvailableIndex(current_index);
				}
				current_index++;
			}
			return list;
		}
	}
	
}
