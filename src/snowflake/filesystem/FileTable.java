package snowflake.filesystem;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import j3l.util.Checker;
import j3l.util.InputUtility;
import j3l.util.LongRange;
import snowflake.GlobalString;
import snowflake.api.DataPointer;
import snowflake.api.FileSystemException;
import snowflake.api.IFlake;

/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.09.22_0
 * @author Johannes B. Latzel
 */
public final class FileTable extends FileSystemDataTable<File, FileData> {
	
	
	/**
	 * <p></p>
	 * 
	 * @param
	 */
	public FileTable(IFlake table_flake) throws IOException {
		super(table_flake, FileData.FILE_DATA_LENGTH);
	}
	
	
	/* (non-Javadoc)
	 * @see j3l.util.DataTable#saveEntry(j3l.util.Indexable)
	 */
	@Override public void saveEntry(File file) throws IOException {
		Checker.checkForValidation(file, GlobalString.File.toString());
		long index = file.getIndex();
		synchronized( flake_output_stream ) {
			long position = index * FileData.FILE_DATA_LENGTH;
			if( position < 0 ) {
				throw new FileSystemException("The index " + index + " is too big!");
			}
			flake_output_stream.getDataPointer().setPosition(position);
			flake_output_stream.write(FileData.getBinaryData(
				FileData.createBuffer(),
				file.getAttributeFlakeIdentification(),
				file.getDataFlakeIdentification(),
				file.getParentDirectory().getIdentification(),
				file.isEmpty()
			));
		}
	}
	
	
	/* (non-Javadoc)
	 * @see j3l.util.DataTable#deleteEntry(j3l.util.Indexable)
	 */
	@Override public void deleteEntry(File file) throws IOException {
		Checker.checkForValidation(file, GlobalString.File.toString());
		long index = file.getIndex();
		synchronized( flake_output_stream ) {
			long position = index * FileData.FILE_DATA_LENGTH;
			if( position < 0 ) {
				throw new FileSystemException("The index " + index + " is too big!");
			}
			flake_output_stream.getDataPointer().setPosition(position);
			flake_output_stream.write(clear_buffer);
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
	@Override public ArrayList<FileData> getAllEntries() throws IOException {
		synchronized( flake_input_stream ) {
			DataPointer pointer = flake_input_stream.getDataPointer();
			pointer.setPosition(0);
			long remaining_bytes = pointer.getRemainingBytes();
			int file_data_length = FileData.FILE_DATA_LENGTH;
			if( remaining_bytes % file_data_length != 0 ) {
				throw new IOException("The file_data has an invalid length: " + remaining_bytes);
			}
			long number_of_entries = remaining_bytes / file_data_length;
			ArrayList<FileData> list = new ArrayList<>(
				// cast is okay, because number_of_entries is smaller than equals Integer.MAX_VALUE
				number_of_entries > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int)number_of_entries
			);
			ByteBuffer buffer = FileData.createBuffer();
			long current_index = 0;
			while( !pointer.isEOF() ) {
				if( !Checker.checkAllElements(InputUtility.readComplete(flake_input_stream, buffer), (byte)0) ) {
					buffer.rewind();
					list.add(new FileData(buffer, current_index));
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
