package snowflake.core.data;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import j3l.util.check.ArgumentChecker;
import snowflake.api.storage.StorageException;


/**
 * <p></p>
 * 
 * @since JDK 1.8
 * @version 2016.02.23_0
 * @author Johannes B. Latzel
 */
public final class DataTable<T extends IBinaryData> {
	
	
	/**
	 * <p></p>
	 */
	private final RandomAccessFile table_file;
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 */
	public DataTable(File table_file) {
		ArgumentChecker.checkForNull(table_file, "table_file");
		if( !table_file.isFile() ) {
			throw new IllegalArgumentException("The table_file must exist and be a file!");
		}
		try {
			this.table_file = new RandomAccessFile(table_file, "rw");
		}
		catch( FileNotFoundException e ) {
			throw new StorageException("The table_file could not be found!", e);
		}
	}
	
	
	/**
	 * <p></p>
	 *
	 * @param
	 * @return
	 * @throws IOException 
	 */
	public void addEntry(BinaryDataWrapper<T> wrapper) {
		if( wrapper == null ) {
			return;
		}
		IBinaryData data = wrapper.getBinaryData();
		byte[] buffer = new byte[data.getDataLength()];
		try {
			data.getBinaryData(buffer);
			synchronized( table_file ) {
				table_file.seek( buffer.length * wrapper.getTableIndex() );
				table_file.write(buffer);
			}
		}
		catch( IOException e ) {
			throw new StorageException("Could not flush the data in this table! Stopped at wrapper \""
					+ wrapper.toString() + "\".", e);
		}
	}

}
